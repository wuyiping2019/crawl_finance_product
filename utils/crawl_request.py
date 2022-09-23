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

        self.__connection = None
        self.__cursor = None

        console_handle = logging.StreamHandler()
        self.logger.addHandler(console_handle)
        self.logger.setLevel(self.log_level)

        self.kwargs = kwargs

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
        return row

    def _row_key_mapping(self, row: dict, field_name_2_new_field_name: dict = None) -> dict:
        """
        对k/v对中的k转换
        :param row:
        :param field_name_2_new_field_name:
        :return:
        """
        if field_name_2_new_field_name is None:
            return row
        new_row = {}
        for k, v in row.items():
            if k in field_name_2_new_field_name.keys():
                new_row[field_name_2_new_field_name[k]] = v

        # 额外添加
        new_row['logId'] = self.log_id
        new_row['getLocalDate'] = getLocalDate()
        return new_row

    def _row_value_mapping(self, row: dict, field_value_mapping: Dict[str, object] = None):
        """
        对k/v中的v转换
        :param row:
        :param field_value_mapping:
        :return:
        """
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

    @abstractmethod
    def _do_crawl(self, session: Session):
        self.logger.info("设置请求参数:")
        self._next_request()
        self.logger.info(f"当前请求参数:{json.dumps(self.request).encode().decode('unicode_escape')}")
        if self.end_flag:
            return
        self.logger.info("开始执行请求:")
        response = session.request(**self.request)
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        resp_str = resp_str.replace('\n', '')
        self.logger.info(
            f"当前请求结果:"
            f"{resp_str}"
        )
        time.sleep(self.sleep_second)
        self.end_flag = self._if_end(response)
        self.logger.info(f"判断当前请求之后是否需要终止爬取:{self.end_flag}")
        if self.end_flag:
            # 如果触发了终止条件
            # 1.如果需要继续访问
            if self.go_on:
                self.logger.info(f"当前请求处理之后终止爬取")
                pass
            else:
                self.logger.info(f"忽略当前请求并终止爬取")
                return
        rows: List[dict] = self._parse_response(response)
        self.logger.info(f"将请求结果转换为列表:{json.dumps(rows).encode().decode('unicode_escape')}")
        self.logger.info(f"开始对每一行数据进行转换")
        for row in rows:
            self.logger.info(f"开始转换：{json.dumps(row).encode().decode('unicode_escape')}")
            self.logger.info(f"调用_row_value_mapping转换value:")
            new_row = self._row_value_mapping(row, self.field_value_mapping)
            self.logger.info(f"转换结果:{json.dumps(new_row).encode().decode('unicode_escape')}")
            self.logger.info(f"调用_row_processor转换value:")
            new_row = self._row_processor(new_row)
            self.logger.info(f"转换结果:{json.dumps(new_row).encode().decode('unicode_escape')}")
            self.logger.info(f"调用_row_key_mapping转换key:")
            new_row = self._row_key_mapping(new_row, self.filed_name_2_new_field_name)
            new_row = self._row_post_processor(new_row)
            self.logger.info(f"转换结果:{json.dumps(new_row).encode().decode('unicode_escape')}")
            self.processed_rows.append(new_row)

    def do_crawl(self):
        self.logger.info('开始准备爬取数据:')
        while not self.end_flag:
            self._do_crawl(self.session)

    def do_save(self, check_props: list):
        try:
            self._do_save(check_props)
            self.__connection.commit()
        except Exception as e:
            self.logger.error(f"WARN:{getLocalDate()}:保存数据失败")
        finally:
            if self.__cursor:
                self.__cursor.close()
            if self.__connection:
                self.__connection.close()

    def _do_save(self, check_props: list):
        """
        存储数据 保证一个_do_save在一个事务之中
        :param cursor:
        :param check_props:
        :return:
        """
        if not (self.__connection and self.__cursor):
            self.logger.info(f"INFO:{getLocalDate()}:开始打开数据连接用于保存数据")
            self.__connection = self.db_poll.connection()
            self.__cursor = self.__connection.cursor()
            self.logger.info(f"INFO:{getLocalDate()}:成功打开数据库连接")
        check_table_exists_connection, check_table_exists_cursor = None, None
        try:
            check_table_exists_connection = self.db_poll.connection()
            check_table_exists_cursor = check_table_exists_connection.cursor()
            flag = check_table_exists(get_table_name(self.identifier), check_table_exists_cursor)
            self.logger.info(
                f"INFO:{getLocalDate()}:检测目标表是否存在{get_table_name(self.identifier)}:{'存在' if flag else '不存在'}")
            if not flag and self.processed_rows:
                row_sample = self.processed_rows[0]
                row_sample['logId'] = 1
                row_sample['createTime'] = getLocalDate()
                create_table(row_sample,
                             self.table_str_type,
                             self.table_number_type,
                             get_table_name(self.identifier),
                             check_table_exists_cursor,
                             get_sequence_name(self.identifier),
                             get_trigger_name(self.identifier),
                             self.db_type)
                check_table_exists_connection.commit()
                self.logger.info(f"INFO:{getLocalDate()}:创建目标表{get_table_name(self.identifier)}成功")
        except Exception as e:
            self.logger.error(f"WARN:{getLocalDate()}:创建目标表{get_table_name(self.identifier)}失败")
        finally:
            if check_table_exists_cursor:
                check_table_exists_connection.close()
            if check_table_exists_connection:
                check_table_exists_connection.close()
        self.logger.info(f"INFO:{getLocalDate()}:开始保存数据")
        for row in self.processed_rows:
            try:
                self._save_row(cursor=self.__cursor,
                               row=row,
                               check_props={k: row[k] for k in check_props})
                self.logger.info(f"INFO:{getLocalDate()}:成功保存{row}")
            except Exception as e:
                self.logger.error(f"ERROR:{getLocalDate()}:crawl_request.CrawlRequest._do_save保存数据失败")
        self.processed_rows = []

    def _save_row(self, cursor, row: dict, check_props: dict):
        """
        存储数据
        :param cursor:
        :param row:
        :param check_props:
        :return:
        """
        try:
            update_else_insert_to_db(cursor=cursor,
                                     target_table=get_table_name(self.identifier),
                                     props_dict=row,
                                     check_props=check_props,
                                     db_type=self.db_type,
                                     logger=self.logger)
        except Exception as e:
            exception = cast_exception(e)
            if isinstance(exception, CustomException):
                if exception.code == 1:
                    add_fields(cursor=cursor,
                               target_table=get_table_name(self.identifier),
                               fields=list(row.keys()),
                               filed_type=self.table_str_type)
                    self._save_row(cursor, row, check_props)

            else:
                self.logger.error(f"WARN:{getLocalDate()}:crawl_request.CrawlRequest._save_row保存数据失败{str(row)}")
                self.logger.error(f"WARN:{getLocalDate()}:crawl_request.CrawlRequest._save_row{str(exception)}")
                raise exception
