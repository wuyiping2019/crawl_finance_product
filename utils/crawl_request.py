import json
import logging
import time
import types
from abc import abstractmethod, ABC
from logging import Logger
from typing import Dict, List

from dbutils.pooled_db import PooledDB
from requests import Session, Response
from utils.custom_exception import cast_exception, CustomException
from utils.db_utils import create_table, update_else_insert_to_db, add_fields, check_table_exists
from utils.global_config import DB_ENV, DBType, get_sequence_name, get_trigger_name, get_table_name
from utils.mark_log import getLocalDate


class CrawlRequestException(Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg


class AbstractCrawlRequest:
    def __init__(self,
                 db_poll: PooledDB,
                 session: Session,
                 request: dict,
                 identifier: str,
                 log_id: int,
                 table_str_type: str = 'clob' if DB_ENV.name == DBType.oracle.name else 'text' if DB_ENV.name == DBType.mysql.name else None,
                 table_number_type: str = 'number(11)' if DB_ENV.name == DBType.oracle.name else 'long' if DB_ENV.name == DBType.mysql.name else None,
                 db_type: DBType = DB_ENV,
                 filed_name_2_new_field_name=None,
                 field_value_mapping=None,
                 mark_code_mapping_count: int = 1,
                 go_on: bool = False,
                 sleep_second=3,
                 logger: Logger = None,
                 log_level=None,
                 check_props: List[str] = None,
                 **kwargs
                 ):
        self.db_poll = db_poll
        self.session = session
        self.mark_code_mapping = None
        self.mark_code_mapping_count = mark_code_mapping_count
        self.filed_name_2_new_field_name = filed_name_2_new_field_name
        self.field_value_mapping = field_value_mapping
        self.end_flag = False
        self.request = request
        self.go_on = go_on
        self.processed_rows = []
        self.db_type = db_type
        self.table_str_type = table_str_type
        self.table_number_type = table_number_type
        self.identifier = identifier
        self.sleep_second = sleep_second
        self.log_id = log_id
        self.logger = logger
        self.log_level = log_level
        self.check_props = check_props
        self._prep_request_flag = False
        self._do_crawl_flag = True

        self.__connection = None
        self.__cursor = None

        console_handle = logging.StreamHandler()
        self.logger.addHandler(console_handle)
        self.logger.setLevel(self.log_level)

        self.kwargs = kwargs

    @abstractmethod
    def _prep_request(self):
        """
        请求之前的准备工作
        """
        pass

    def _do_request(self) -> Response:
        """
        执行请求获取相应结果
        :return:
        """
        response = self.session.request(**self.request)
        return response

    @abstractmethod
    def _parse_response(self, response: Response) -> List[dict]:
        pass

    @abstractmethod
    def _row_processor(self, row: dict) -> dict:
        pass

    def _row_key_mapping(self, row: dict, field_name_2_new_field_name: dict = None) -> dict:
        """
        对k/v对中的k转换
        :param row:
        :param field_name_2_new_field_name:
        :return:
        """
        if self.logger:
            self.logger.info(f"INFO:{getLocalDate()}:crawl_request.CrawlRequest._row_key_mapping:开始对{row}进行的key进行转换")
        if field_name_2_new_field_name is None:
            return row
        new_row = {}
        for k, v in row.items():
            if k in field_name_2_new_field_name.keys():
                new_row[field_name_2_new_field_name[k]] = v
        if self.logger:
            self.logger.info(f"INFO:{getLocalDate()}:crawl_request.CrawlRequest._row_key_mapping:key的转换结果{new_row}")
        return new_row

    def _row_value_mapping(self, row: dict, field_value_mapping: Dict[str, object] = None):
        """
        对k/v中的v转换
        :param row:
        :param field_value_mapping:
        :return:
        """
        if self.logger:
            self.logger.info(f"INFO:{getLocalDate()}:开始对{row}的value进行转换")
        if field_value_mapping is None:
            return row
        new_row = {}
        for k, v in row.items():
            if k in field_value_mapping.keys():
                value_mapping = field_value_mapping[k]
                if isinstance(value_mapping, types.LambdaType) or isinstance(value_mapping, types.FunctionType):
                    # 表示需要调用函数进行转换
                    v = value_mapping(v)
                else:
                    if str(v) not in value_mapping.keys():
                        # 记录没有设置的映射编码
                        if k not in self.mark_code_mapping.keys():
                            self.mark_code_mapping[k] = {}
                        if v not in self.mark_code_mapping[k].keys():
                            self.mark_code_mapping[k][v] = []
                        if len(self.mark_code_mapping[k][v]) <= self.mark_code_mapping_count:
                            self.mark_code_mapping[k][v].append(row)
                    v = value_mapping.get(str(v), str(v))
            new_row[k] = v
        if self.logger:
            self.logger.info(f"INFO:{getLocalDate()}:value的转换结果{new_row}")
        return new_row

    @abstractmethod
    def _row_post_processor(self, row: dict):
        return row

    @abstractmethod
    def _if_end(self, response: Response) -> bool:
        """
        更新终止标识
        :param response:
        :return:
        """
        pass

    @abstractmethod
    def _next_request(self):
        """
        更新请求数据
        :return:
        """
        pass

    def _do_crawl(self):
        # 判断end_flag参数 为true的话 直接终止
        if self.end_flag:
            return

        if not self._prep_request():
            self._prep_request()

        if self.logger:
            self.logger.info(f"INFO:{getLocalDate()}:开始设置请求参数")
        # 设置请求参数
        self._next_request()

        if self.logger:
            self.logger.info(f"INFO:{getLocalDate()}:当前请求参数:{self.request}")

        if self.logger:
            self.logger.info(f"INFO:{getLocalDate()}:开始执行请求")

        # 执行请求操作
        response = self._do_request()
        time.sleep(self.sleep_second)

        # 根据请求结果是否需要继续处理
        self.end_flag = self._if_end(response)

        if self.logger:
            self.logger.info(f"INFO:{getLocalDate()}:判断当前请求之后是否需要终止爬取:{self.end_flag}")
        if self.end_flag:
            # 如果触发了终止条件
            # 1.如果需要继续访问
            if self.go_on:
                if self.logger:
                    self.logger.info(f"INFO:{getLocalDate()}:当前请求处理之后终止爬取")
                pass
            else:
                if self.logger:
                    self.logger.info(f"INFO:{getLocalDate()}:忽略当前请求并终止爬取")
                return

        # 将请求结果转换为字典列表
        rows: List[dict] = self._parse_response(response)
        if self.logger:
            self.logger.info(f"INFO:{getLocalDate()}:将请求结果转换为字典列表,具有{len(rows)}条数据")
            self.logger.info(f"INFO:{getLocalDate()}:开始对每一行数据进行转换")
        count = 1
        for row in rows:
            if self.logger:
                self.logger.info(f"INFO:{getLocalDate()}:开始转换第{count}条数据")
            new_row = self._row_transform(row)
            self.processed_rows.append(new_row)
            count += 1

    def _row_transform(self, row: dict):
        new_row = self._row_value_mapping(row, self.field_value_mapping)
        new_row = self._row_processor(new_row)
        new_row = self._row_key_mapping(new_row, self.filed_name_2_new_field_name)
        new_row = self._row_post_processor(new_row)
        return new_row

    def do_crawl(self):
        if not self._do_crawl_flag:
            raise CrawlRequestException(None, f"ERROR:{getLocalDate()}:已经调用close方法,无法再次执行")
        if self.logger:
            self.logger.info(f'INFO:{getLocalDate()}:开始准备爬取数据')
        while not self.end_flag:
            self._do_crawl()

    def do_save(self):
        if not self._do_crawl_flag:
            raise CrawlRequestException(None, f"ERROR:{getLocalDate()}:已经调用close方法,无法再次执行")
        if self.logger:
            self.logger.info(f"INFO:{getLocalDate()}:开始保存数据")
        try:
            self._do_save()
            self.__connection.commit()
        except Exception as e:
            if self.logger:
                self.logger.error(f"WARN:{getLocalDate()}:{e}")
                self.logger.error(f"WARN:{getLocalDate()}:保存数据失败")
            else:
                print(e)
                print("保存数据失败")
        finally:
            if self.__cursor:
                self.__cursor.close()
            if self.__connection:
                self.__connection.close()

    def _do_save(self):
        """
        存储数据 保证一个_do_save在一个事务之中
        :param cursor:
        :param check_props:
        :return:
        """
        if not (self.__connection and self.__cursor):
            if self.logger:
                self.logger.info(f"INFO:{getLocalDate()}:开始打开数据连接用于保存数据")
            self.__connection = self.db_poll.connection()
            self.__cursor = self.__connection.cursor()
            if self.logger:
                self.logger.info(f"INFO:{getLocalDate()}:成功打开数据库连接")
        else:
            self.logger.info(f"INFO:{getLocalDate()}:监测到已连接到数据库")

        check_table_exists_flag = None
        try:
            check_table_exists_flag = check_table_exists(get_table_name(self.identifier), self.__cursor)
            self.logger.info(
                f"INFO:{getLocalDate()}:检测目标表是否存在{get_table_name(self.identifier)}:{'存在' if check_table_exists_flag else '不存在'}")
        except Exception as e:
            if self.logger:
                self.logger.error(
                    f"ERROR:{getLocalDate()}:crawl_request.CrawlRequest._do_save/db_utils.check_table_exists错误")
                self.logger.error(f"ERROR:{getLocalDate()}:{e}")
            else:
                raise CrawlRequestException(None,
                                            f"ERROR:{getLocalDate()}:crawl_request.CrawlRequest._do_save/db_utils.check_table_exists检测目标表是否存在报错:{e}")
        if not check_table_exists_flag and self.processed_rows:
            if self.logger:
                f"INFO:{getLocalDate()}:开始创建表{get_table_name(self.identifier)}"
            row_sample = self.processed_rows[0]
            row_sample['logId'] = 1
            row_sample['createTime'] = getLocalDate()
            try:
                create_flag = create_table(row_sample,
                                           self.table_str_type,
                                           self.table_number_type,
                                           get_table_name(self.identifier),
                                           self.__cursor,
                                           get_sequence_name(self.identifier),
                                           get_trigger_name(self.identifier),
                                           self.db_type)
                if create_flag:
                    self.logger.info(f"INFO:{getLocalDate()}:成功创建{get_table_name(self.identifier)}表")
            except Exception as e:
                if self.logger:
                    self.logger.error(
                        f"ERROR:{getLocalDate()}:crawl_request._do_save/db_utils.create_table创建表{get_table_name(self.identifier)}失败")
                    self.logger.error(f"ERROR:{getLocalDate()}:{e}")
                else:
                    raise CrawlRequestException(None,
                                                f"ERROR:{getLocalDate()}:crawl_request.CrawlRequest._do_save/db_utils.create_table创建{get_table_name(self.identifier)}表失败:{e}")

        if self.logger:
            self.logger.info(f"INFO:{getLocalDate()}:开始保存数据")
        for row in self.processed_rows:
            check_prosp_dict = {}
            try:
                check_prosp_dict = {k: row[k] for k in self.check_props}
            except Exception as e:
                if self.logger:
                    self.logger.error(f"ERROR:{getLocalDate()}:crawl_request.CrawlRequest.check_props约束属性不存在于{row}中")
                    self.logger.error(f"ERROR:{getLocalDate()}:数据保存失败：{row}")
                    continue
                else:
                    raise CrawlRequestException(None,
                                                f"ERROR:{getLocalDate()}:crawl_request.CrawlRequest.check_props不存在于{row}中")
            try:
                self._save_row(row=row, check_props_dict=check_prosp_dict)
                if self.logger:
                    self.logger.info(f"INFO:{getLocalDate()}:成功保存{row}")
            except Exception as e:
                if self.logger:
                    self.logger.error(f"ERROR:{getLocalDate()}:crawl_request.CrawlRequest._do_save保存数据失败")
                else:
                    raise CrawlRequestException(None, f"crawl_request.CrawlRequest._do_save保存数据失败")
        self.processed_rows = []

    def _save_row(self, row: dict, check_props_dict: dict):
        """
        存储数据
        :param cursor:
        :param row:
        :param check_props:
        :return:
        """
        try:
            update_else_insert_to_db(cursor=self.__cursor,
                                     target_table=get_table_name(self.identifier),
                                     props_dict=row,
                                     check_props=check_props_dict,
                                     db_type=self.db_type,
                                     logger=self.logger)
        except Exception as e:
            exception = cast_exception(e)
            if isinstance(exception, CustomException):
                if exception.code == 1:
                    add_fields(cursor=self.__cursor,
                               target_table=get_table_name(self.identifier),
                               fields=list(row.keys()),
                               filed_type=self.table_str_type)
                    self._save_row(row, check_props_dict)

            else:
                raise exception

    def close(self):
        if self.session:
            self.session.close()
        if self.__cursor and self.__connection:
            self.__cursor.close
            self.__connection.close
        if self._do_crawl_flag:
            self._do_crawl_flag = False
        else:
            raise CrawlRequestException(None, f"ERROR:{getLocalDate()}:close方法仅能调用一次")
