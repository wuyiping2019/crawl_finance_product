import json
import logging
import sys
import time
import types
from abc import abstractmethod, ABC
from logging import Logger
from typing import Dict, List

from dbutils.pooled_db import PooledDB
from requests import Session, Response

from crawl import MutiThreadCrawl
from utils.custom_exception import cast_exception, CustomException
from utils.db_utils import create_table, update_else_insert_to_db, add_fields, check_table_exists
from utils.global_config import DB_ENV, DBType, get_sequence_name, get_trigger_name, get_table_name
from utils.logging_utils import get_logger, log
from utils.mappings import FIELD_MAPPINGS
from utils.mark_log import getLocalDate


class CrawlRequestException(Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg


class AbstractCrawlRequest:
    def __init__(self,
                 request: dict = None,
                 identifier: str = None,
                 muti_thread_crawl: MutiThreadCrawl = None,
                 db_poll: PooledDB = None,
                 session: Session = None,
                 log_id: int = None,
                 table_str_type: str = 'clob' if DB_ENV.name == DBType.oracle.name else 'text' if DB_ENV.name == DBType.mysql.name else None,
                 table_number_type: str = 'number(11)' if DB_ENV.name == DBType.oracle.name else 'long' if DB_ENV.name == DBType.mysql.name else None,
                 db_type: DBType = DB_ENV,
                 field_name_2_new_field_name=None,
                 field_value_mapping=None,
                 mark_code_mapping_count: int = 1,
                 go_on: bool = False,
                 sleep_second=3,
                 check_props: List[str] = None,
                 **kwargs
                 ):
        self.db_poll = db_poll
        self.session = session
        self.mark_code_mapping = None
        self.mark_code_mapping_count = mark_code_mapping_count
        self.field_name_2_new_field_name = field_name_2_new_field_name
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
        self.check_props = check_props
        self._prep_request_flag = False
        self._do_crawl_flag = True

        self.__connection = None
        self.__cursor = None

        self.muti_thread_crawl = muti_thread_crawl

        self.kwargs = kwargs

    def init_props(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
                if k == 'muti_thread_crawl':
                    current_module_name = sys.modules[__name__].__name__
                    config_dict = getattr(v, 'config_dict')
                    self.logger = get_logger(log_name=current_module_name,
                                             log_level=config_dict['logger.level'],
                                             log_modules=config_dict['logger.modules'],
                                             module_name=current_module_name,
                                             )

    @abstractmethod
    def _prep_request(self):
        """
        请求之前的准备工作
        仅执行一次
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
        where = 'crawl_request.CrawlRequest._row_key_mapping'
        """
        对k/v对中的k转换
        :param row:
        :param field_name_2_new_field_name:
        :return:
        """
        log(self.logger, 'debug', where, f'开始对key进行转换:{row}')
        if field_name_2_new_field_name is None:
            return row
        new_row = {}
        for k, v in row.items():
            if k in field_name_2_new_field_name.keys():
                new_row[field_name_2_new_field_name[k]] = v
            elif k in FIELD_MAPPINGS.values():
                new_row[k] = v
            else:
                pass
        log(self.logger, 'debug', where, f'value转换结果:{new_row}')
        return new_row

    def _row_value_mapping(self, row: dict, field_value_mapping: Dict[str, object] = None):
        """
        对k/v中的v转换
        :param row:
        :param field_value_mapping:
        :return:
        """
        where = 'crawl_request.CrawlRequest._row_value_mapping'
        log(self.logger, 'debug', where, f'开始对value进行转换:{row}')
        if field_value_mapping is None:
            return row
        new_row = {}
        for k, v in row.items():
            mapping_v = v
            # 表示配置了需要进行value转换
            if k in field_value_mapping.keys():
                value_mapping = field_value_mapping[k]
                if isinstance(value_mapping, types.LambdaType) or isinstance(value_mapping, types.FunctionType):
                    # 表示需要调用函数进行转换
                    mapping_v = value_mapping(v)
                elif isinstance(value_mapping, dict):
                    mapping_v = value_mapping.get(str(v), str(v))
                    # 表示使用字典进行映射
                    if str(v) not in value_mapping.keys():
                        log(self.logger, 'warn', where, f"{k}:{str(v)}进行value转换,该value不存在于{value_mapping}中")
                        # 记录没有设置的映射编码
                        if not self.mark_code_mapping:
                            self.mark_code_mapping = {}
                        if k not in self.mark_code_mapping.keys():
                            self.mark_code_mapping[k] = {}
                        if v not in self.mark_code_mapping[k].keys():
                            self.mark_code_mapping[k][v] = []
                        if len(self.mark_code_mapping[k][v]) <= self.mark_code_mapping_count:
                            self.mark_code_mapping[k][v].append(row)
            new_row[k] = mapping_v
        log(self.logger, 'debug', where, f'value转换结果:{new_row}')
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

    def _do_crawl2(self):
        where = 'crawl_request.CrawlRequest._do_crawl'
        if not self._prep_request_flag:
            log(self.logger, 'debug', where, f"执行爬取工作之前的处理_prep_request")
            self._prep_request()
        log(self.logger, 'debug', where, '设置请求参数')
        # 设置将要执行的请求状态数据
        self._next_request()
        get_params_info = lambda \
                x: f"当前请求的{x}:{self.request[x] if self.request.get(x, None) else None}" if self.request.get(
            x, None) else ''
        log(self.logger, 'info', where,
            f'{get_params_info("url")} {get_params_info("data")} {get_params_info("json")} {get_params_info("method")}')
        # 执行请求操作
        log(self.logger, 'debug', where, '执行请求操作')
        response = self._do_request()
        log(self.logger, 'info', where, f'请求响应结果:{response.status_code}')
        time.sleep(self.sleep_second)
        rows: List[dict] = self._parse_response(response)
        log(self.logger, 'info', where, f"解析请求结果:{len(rows)}条数据")
        count = 1
        for row in rows:
            log(self.logger, 'debug', where, f'开始对第{count}条数据进行处理')
            new_row = {}
            try:
                new_row = self._row_transform(row)
            except Exception as e:
                log(self.logger, 'error', where, f"转换数据报错:{row}")
                log(self.logger, 'error', where, f"{e}")
            log(self.logger, 'debug', where, f"处理结果:{new_row}")
            self.processed_rows.append(new_row)
            log(self.logger, 'debug', where, f'本次爬虫已处理{len(self.processed_rows)}条数据')
            count += 1
        self.end_flag = self._if_end(response)
        if self.end_flag:
            return
        self._next_request()

    def _do_crawl(self):
        where = 'crawl_request.CrawlRequest._do_crawl'
        # 判断end_flag参数 为true的话 直接终止
        log(self.logger, 'debug', where, f'判断end_flag终止标识:{self.end_flag}')
        if self.end_flag:
            return
        log(self.logger, 'debug', where, f"判断开始爬取工作之前的处理标识:{self._prep_request_flag}")
        if not self._prep_request_flag:
            log(self.logger, 'debug', where, f"执行爬取工作之前的处理_prep_request")
            self._prep_request()

        log(self.logger, 'debug', where, '设置请求参数')
        self._next_request()
        log(self.logger, 'info', where,
            f'当前请求的url:{self.request["url"]} 当前请求的body:{self.request["json"] if self.request.get("json", None) else self.request["data"]}')
        # 执行请求操作
        log(self.logger, 'debug', where, '执行请求操作')
        response = self._do_request()
        log(self.logger, 'info', where, f'请求响应结果:{response.status_code}')
        time.sleep(self.sleep_second)

        # 根据请求结果是否需要继续处理
        log(self.logger, 'debug', where, f'根据条件设置end_flag标识')
        self.end_flag = self._if_end(response)
        log(self.logger, 'info', where, f'当前end_flag标识:{self.end_flag}')
        if self.end_flag:
            # 如果触发了终止条件
            # 1.如果需要继续访问
            if self.go_on:
                log(self.logger, 'debug', where, f'该爬虫工作的go_on标识:{self.go_on} 处理完本次请求响应结果后将结束')
                pass
            else:
                log(self.logger, 'debug', where, f"该爬虫工作的go_on标识:{self.go_on} 不需要继续处理本次请求响应结果 即刻结束")
                return

        # 将请求结果转换为字典列表
        rows: List[dict] = self._parse_response(response)
        log(self.logger, 'info', where, f"解析请求结果:{len(rows)}条数据")
        count = 1
        for row in rows:
            log(self.logger, 'debug', where, f'开始对第{count}条数据进行处理')
            new_row = {}
            try:
                new_row = self._row_transform(row)
            except Exception as e:
                log(self.logger, 'error', where, f"转换数据报错:{row}")
                log(self.logger, 'error', where, f"{e}")
            log(self.logger, 'debug', where, f"处理结果:{new_row}")
            self.processed_rows.append(new_row)
            log(self.logger, 'debug', where, f'本次爬虫已处理{len(self.processed_rows)}条数据')
            count += 1

    def _row_transform(self, row: dict):
        new_row = self._row_value_mapping(row, self.field_value_mapping)
        new_row = self._row_processor(new_row)
        new_row = self._row_key_mapping(new_row, self.field_name_2_new_field_name)
        new_row = self._row_post_processor(new_row)
        return new_row

    def do_crawl(self):
        where = 'crawl_request.CrawlRequest.do_crawl'
        if not self._do_crawl_flag:
            raise CrawlRequestException(None, f"ERROR:{getLocalDate()}:已经调用close方法,无法再次执行")

        log(self.logger, 'info', where, '开始准备爬取工作')
        while not self.end_flag:
            self._do_crawl2()

    def do_save(self):
        where = 'crawl_request.CrawlRequest.do_save'
        if not self._do_crawl_flag:
            raise CrawlRequestException(None, f"ERROR:{getLocalDate()}:已经调用close方法,无法再次执行")
        log(self.logger, 'info', where, '开始准备保存工作')
        try:
            self._do_save()
            self.__connection.commit()
        except Exception as e:
            log(self.logger, 'error', where, f"{e}")
            log(self.logger, 'error', where, '保存数据失败')
            raise e
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
        where = 'crawl_request.CrawlRequest._do_save'
        if not (self.__connection and self.__cursor):
            log(self.logger, 'debug', where, '开始打开数据连接用于保存数据')
            self.__connection = self.db_poll.connection()
            self.__cursor = self.__connection.cursor()
            log(self.logger, 'debug', where, '成功打开数据库连接')
        else:
            log(self.logger, 'debug', where, '监测到已连接到数据库')

        check_table_exists_flag = None
        try:
            check_table_exists_flag = check_table_exists(get_table_name(self.identifier), self.__cursor)
            log(self.logger, 'warn', where,
                f"检测目标表是否存在{get_table_name(self.identifier)}:{'存在' if check_table_exists_flag else '不存在'}")
        except Exception as e:
            log(self.logger, 'error', where, f"检测目标表是否存在失败{get_table_name(self.identifier)}")
            log(self.logger, 'error', where, f"{e}")
            raise CrawlRequestException(None, f"检测目标表是否存在失败{get_table_name(self.identifier)}")
        if not check_table_exists_flag and self.processed_rows:
            log(self.logger, 'info', where, f"开始创建表{get_table_name(self.identifier)}")
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
                    log(self.logger, 'info', where, f"成功创建{get_table_name(self.identifier)}表")
            except Exception as e:
                log(self.logger, 'error', where, f"创建表{get_table_name(self.identifier)}失败")
                raise CrawlRequestException(None, f"创建表{get_table_name(self.identifier)}失败")

        log(self.logger, 'info', where, '开始保存数据')
        for row in self.processed_rows:
            if not row:
                continue
            check_prosp_dict = {}
            try:
                check_prosp_dict = {k: row[k] for k in self.check_props}
            except Exception as e:
                log(self.logger, 'error', where, f"check_props:{self.check_props}约束属性不存在于{row}中")
                log(self.logger, 'error', where, '数据保存失败')
                raise CrawlRequestException(None, f"check_props:{self.check_props}约束属性不存在于{row}中")
            try:
                self._save_row(row=row, check_props_dict=check_prosp_dict)
                log(self.logger, 'debug', where, f"成功保存{row}")
            except Exception as e:
                log(self.logger, 'debug', where, f"保存失败{row}")
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
            log_modules = self.muti_thread_crawl.config_dict['logger.modules']
            log_level = self.muti_thread_crawl.config_dict['logger.level']
            logger = get_logger('utils.db_utils', log_level, log_modules, 'utils.db_utils')
            update_else_insert_to_db(cursor=self.__cursor,
                                     target_table=get_table_name(self.identifier),
                                     props_dict=row,
                                     check_props=check_props_dict,
                                     db_type=self.db_type,
                                     logger=logger
                                     )
        except Exception as e:
            exception = cast_exception(e)
            if isinstance(exception, CustomException):
                if exception.code == 1:
                    log(self.logger, 'warn', 'crawl_request', '更新或插入的字段不存在')
                    add_fields(cursor=self.__cursor,
                               target_table=get_table_name(self.identifier),
                               fields=list(row.keys()),
                               filed_type=self.table_str_type)
                    log(self.logger, 'info', 'crawl_request', '添加不存在的字段')
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
