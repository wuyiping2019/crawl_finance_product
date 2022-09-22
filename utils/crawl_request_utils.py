import time
from typing import Dict, List, Union

import requests
from dbutils.pooled_db import PooledDB, PooledSharedDBConnection, PooledDedicatedDBConnection
from requests import Session
from utils.common_utils import delete_empty_value
from utils.custom_exception import cast_exception, CustomException
from utils.db_utils import create_table, get_conn, update_else_insert_to_db, add_fields, check_table_exists, close
from utils.global_config import DB_ENV, DBType, LOG_TABLE
from utils.mappings import FIELD_MAPPINGS
from utils.mark_log import getLocalDate, mark_failure_log


class CrawlRequestException(Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg


class CrawlRequest:
    def __init__(self,
                 method,
                 url,
                 switch_page_func=None,
                 params=None,
                 data=None,
                 json=None,
                 headers=None,
                 field_mapping_funcs: Dict[str, callable] = None,
                 mapping_dicts: Dict[str, Dict[str, str]] = None,
                 transformed_keys: Dict[str, str] = None,
                 missing_code_sample=3,
                 target_table: str = None,
                 sequence_name: str = None,
                 trigger_name: str = None,
                 db_type: DBType = DB_ENV,
                 table_str_type: str = 'clob',
                 table_number_type: str = 'number(11)',
                 log_id=None,
                 session=None,
                 to_rows: callable = None,
                 total_page: int = None,
                 db_poll: PooledDB = None,
                 sleep_second: int = 1,
                 end_func: callable = None,
                 row_post_processor_func=None,
                 # 允许在transormed_key中value允许传入的value值 即对应FIELD_MAPPINGS中的key值
                 # 这些value值不会使用使用FIELD_MAPPINGS字典进行转换
                 # 而是样本输出在转换之后的字典中
                 # 最终加入到processed_rows列表中之前 会删除这些key值
                 temp_transformed_keys: List[str] = [],
                 post_requests: list = [],
                 **kwargs):
        self.request = self.__do_init(method, url, params, data, json, headers, **kwargs)
        self.mark_code_mapping = {}
        self.missing_code_sample = missing_code_sample
        self.field_mapping_funcs = field_mapping_funcs
        self.mapping_dicts = mapping_dicts
        self.transformed_keys = transformed_keys
        self.target_table = target_table
        self.sequence_name = sequence_name
        self.trigger_name = trigger_name
        self.db_type = db_type
        self.processed_rows = []
        self.table_exists = False
        self.table_str_type = table_str_type
        self.table_number_type = table_number_type
        self.log_id = log_id
        self.to_rows = to_rows
        self.sleep_second = sleep_second
        self.db_poll = db_poll
        self.page_no = None
        # 总页数
        # 当无法获取总页数时,可以为None
        # 此时就需要其他的方法来判断是否爬取终止-使用end_func函数来判断
        self.total_page = total_page
        # 该属性来标识终止爬虫
        self.end_flag = False
        # 调用该方法切换下一页的请求 形参是self
        # 在调用这个方法之前 自动调用对self.page_no + 1
        self.switch_page_func: callable = switch_page_func
        # 有时候无法直接获取总页数 从而根据爬取的页数来终止爬虫
        # 如果该方法赋值 则每次请求中都会调用这个方法来更新self.end_flag参数
        # self.end_func(self,response)
        self.end_func = end_func
        self.row_post_processor_func = row_post_processor_func
        self.temp_transformed_keys = temp_transformed_keys
        self.post_requests = post_requests
        if not session:
            self.session = requests.session()
        else:
            self.session = session

    def do_crawl(self):
        # 切换请求数据中的page_no
        self.switch_page()
        if self.switch_page_func:
            self.switch_page_func(self)
        response = self.session.request(**self.request)
        # 终止继续爬取数据的条件：在end_func函数中更新self.end_flag
        if self.end_func:
            # 该函数的形参是自身和本次响应结果
            self.end_func(self, response)
        time.sleep(self.sleep_second)
        rows: List[dict] = self.to_rows(response)
        processed_rows = []
        for row in rows:
            processed_row = self.__do_row_mapping(row)
            if processed_row:
                # 删除临时配置的temp_key
                if self.temp_transformed_keys:
                    for temp_key in self.temp_transformed_keys:
                        if temp_key in processed_row.keys():
                            del processed_row[temp_key]
                processed_rows.append(processed_row)
        self.processed_rows += processed_rows
        print(f"当前处理{self.page_no}/{self.total_page}")
        # 终止继续爬取数据的条件:当page_no与total_page相等时 终止
        if self.page_no == self.total_page and self.page_no and self.total_page:
            self.end_flag = True

    def do_save(self):
        try:
            # 如果这个CrawlRequet对象调用了do_save方法 则该方法处于一个事务之中
            # 在__do_save中释放了资源
            connection = self.db_poll.connection()
            self.__do_save(connection)
            return True
        except Exception as e:
            print(CrawlRequestException(None, f'do_save保存失败:{str(e)}'))
            return False

    def __do_save(self, connection=None):
        """
        执行存储操作 整个存储操作在一个事务之中
        :return:
        """

        # 如果爬虫数据结果 没有数据 则抛出空数据异常
        if not self.processed_rows:
            raise CrawlRequestException(None, 'crawl_request_utils:__do_save保存的数据为空列表')
        # 如果self.connection对象为空 则抛出无法获取连接异常
        if not connection:
            raise CrawlRequestException(None, 'crawl_request_utils:__do_save无法从数据连接池中获取连接')

        # 如果不存在目标表 则创建表
        if not self.table_exists:
            # 创建表时单独从连接池中获取一个连接
            create_table_conn = self.db_poll.connection()
            create_table_cursor = create_table_conn.cursor()
            # 检测表是否存在
            flag = check_table_exists(self.target_table, create_table_cursor)
            if not flag:
                try:
                    create_table(self.processed_rows[0],
                                 self.table_str_type,
                                 self.table_number_type,
                                 self.target_table,
                                 create_table_cursor,
                                 self.sequence_name,
                                 self.trigger_name)
                    # 创建成功提交事务
                    create_table_conn.commit()
                    self.table_exists = True
                except Exception as e:
                    # 表创建失败 则回滚事务
                    create_table_conn.rollback()
                    # 抛出创建表失败异常
                    raise CrawlRequestException(None, f'crawl_request_utils:__do_save:创建表{self.target_table}失败')
                finally:
                    # 释放资源
                    close([create_table_cursor, create_table_conn])

        # CrawlRequest中的connection对象用于操作存储数据的操作
        cursor = connection.cursor()
        try:
            for processed_row in self.processed_rows:
                processed_row['logId'] = self.log_id
                processed_row['createTime'] = getLocalDate()
                delete_empty_value(processed_row)
                # 执行插入操作
                self.__save_row(cursor, self.target_table, processed_row, self.db_type, self.table_str_type)
            connection.commit()
            self.processed_rows = []
        except Exception as e:
            connection.rollback()
            raise CrawlRequestException(None, f"crawl_request_utils:__do_save执行插入操作失败:\n{str(e)}")
        finally:
            close([cursor, connection])

    def __do_init(self, method, url, params=None, data=None, json=None, headers=None, **kwargs):
        request_params = {
            'method': method,
            'url': url,
            'params': params,
            'data': data,
            'json': json,
            'headers': headers
        }
        for k, v in kwargs.items():
            if k:
                request_params[k] = v
        # 删除空key/value
        delete_empty_value(request_params)
        return request_params

    def __do_row_mapping(self, row: dict) -> dict:
        """
        针对从响应数据中获取的一个dict的数据进行转换
        :param row: 获取的原始数据
        :return: 返回转换之后的数据
        """
        processed_row = {}
        for k, v in row.items():
            if k not in self.transformed_keys.keys():
                continue
            # 对原始数据字典中的每个key进行处理
            temp_dict = self.__field_mapping(row,
                                             k,
                                             self.field_mapping_funcs.get(k, None),
                                             self.mapping_dicts.get(k, None),
                                             self.transformed_keys.get(k, None)
                                             )
            processed_row.update(temp_dict)
        delete_empty_value(processed_row)
        # 如果设置了后处理器 则继续对处理之后的数据进行处理
        if self.row_post_processor_func:
            self.row_post_processor_func(self, processed_row)
        return processed_row

    def __field_mapping(self,
                        row: dict,
                        row_field: str,
                        func: callable = None,
                        mapping_dict: Dict[str, str] = None,
                        transformed_key: str = None) -> dict:
        """
        对一个key/value进行转换
        返回单个键值对的字典
        :param row: 爬取的原始字典数据
        :param row_field: 需要转换的字段
        :param func: 转换字典使用的函数
        :param mapping_dict: 转换字典需要使用的映射字典
        :param transformed_key: 转换之后使用的FIELD_MAPPINGS中的key值
        :return: 返回单个键值对的dict
        """
        if row_field not in row.keys():
            raise CrawlRequestException(None, f'__field_mapping:(传入的row:{row}中不存在row_field:{row_field}的key值)')
        value = row[row_field]
        if func:
            value = func(value)
        if mapping_dict is not None and value in mapping_dict.keys():
            value = mapping_dict[value]
        if mapping_dict is not None and value not in mapping_dict.keys():
            # 表示该字典需要解码 但是没有配置映射关系
            if row_field not in self.mark_code_mapping:
                self.mark_code_mapping[row_field] = {}
            if value not in self.mark_code_mapping[row_field]:
                self.mark_code_mapping[row_field][value] = []
            if len(self.mark_code_mapping[row_field][value]) < self.missing_code_sample:
                self.mark_code_mapping[row_field][value].append(row)

        if transformed_key in self.temp_transformed_keys:
            return {
                transformed_key: value
            }
        elif transformed_key in FIELD_MAPPINGS.keys():
            return {
                FIELD_MAPPINGS[transformed_key]: value
            }
        else:
            raise CrawlRequestException(None,
                                        f"__field_mapping:{transformed_key}不存在于temp_transformed_keys:{self.temp_transformed_keys}和FIELD_MAPPINGS：{FIELD_MAPPINGS.keys()}")

    def switch_page(self):
        # page_no默认赋值None 表示刚开始爬取数据时 page_no是None 在__init__形参中无法设置page_no,创建的对象page_no就是None
        if not self.page_no:
            self.page_no = 1
        else:
            # 每一次调用该方法 表示更新page_no参数
            self.page_no += 1

    def crawl_and_save(self):
        while not self.end_flag:
            self.do_crawl()
        self.do_save()

    def __save_row(self, cursor, target_table, processed_row, db_type, table_str_type):
        """
        保存一条数据
        :param cursor:
        :param target_table:
        :param processed_row:
        :param db_type:
        :param table_str_type:
        :return:
        """
        try:
            update_else_insert_to_db(cursor,
                                     target_table,
                                     processed_row,
                                     {
                                         'logId': processed_row.get('logId', None),
                                         'cpbm': processed_row.get('cpbm', None)
                                     },
                                     db_type
                                     )
        except Exception as e:
            exception = cast_exception(e)
            if isinstance(exception, CustomException):
                if exception.code == 1:
                    add_fields(cursor,
                               target_table,
                               processed_row.keys(),
                               table_str_type
                               )
                    # 重新执行插入操作
                    self.__save_row(cursor, target_table, processed_row, db_type, table_str_type)
            else:
                exception.message = '\n' + f'crawl_request_utils:__save_row 执行插入操作失败 {str(e)}'
                raise exception
