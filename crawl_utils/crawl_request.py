from __future__ import annotations

import time
import types
from abc import abstractmethod
from typing import List, Dict

import requests
from requests import Response, Session

from crawl_utils.custom_exception import cast_exception, CustomException
from crawl_utils.db_utils import check_table_exists, create_table, update_else_insert_to_db, add_fields
from crawl_utils.global_config import get_table_name, get_sequence_name, get_trigger_name
from crawl_utils.logging_utils import get_logger
from crawl_utils.mappings import FIELD_MAPPINGS
from crawl_utils.mark_log import getLocalDate
from config_parser import CrawlConfig, crawl_config

logger = get_logger(name=__name__,
                    log_level=crawl_config.log_level,
                    log_modules=crawl_config.log_modules,
                    filename=crawl_config.log_filename)


class CrawlRequestException(Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg


class AbstractCrawlRequest:

    @abstractmethod
    def _pre_crawl(self, **kwargs):
        """
        爬虫之前的准备工作 仅调用一次
        :return:
        """
        pass

    @abstractmethod
    def _config_params(self, **kwargs):
        """
        每次执行请求之前都会调用一次
        :return:
        """
        pass

    @abstractmethod
    def _do_request(self, **kwargs):
        """
        执行请求
        :return:
        """
        pass

    @abstractmethod
    def _parse_response(self, **kwargs):
        """
        解析响应结果
        :return:
        """
        pass

    @abstractmethod
    def _config_end_flag(self, **kwargs):
        """
        配置end_flag标识
        :return:
        """
        pass

    @abstractmethod
    def do_save(self, **kwargs):
        """
        执行保存工作
        :return:
        """
        pass

    @abstractmethod
    def close(self, **kwargs):
        """
        释放资源
        :return:
        """
        pass

    @abstractmethod
    def _process_rows(self, **kwargs):
        pass

    @abstractmethod
    def do_crawl(self, **kwargs):
        """
        控制整个爬虫工作流
        :return:
        """
        pass


class SessionRequest:
    def __init__(self,
                 url=None,
                 method=None,
                 json=None,
                 data=None,
                 params=None,
                 headers=None):
        self.url = url
        self.method = method
        self.json = json
        self.data = data
        self.params = params
        self.headers = headers


class RowFilter:
    @abstractmethod
    def filter(self, row: dict) -> dict:
        pass


class RowKVTransformAndFilter(RowFilter):
    """
    针对字典的row进行过滤和处理操作
    """

    def __init__(self, filters: List[str] | Dict[str, object]):
        super().__init__()
        self.filters = filters

    def filter(self, row: dict) -> dict:
        """
               针对row的key进行过滤
               1.当self.filters为空时,返回原字典
               2.当self.filters为列表时,针对row中的key进行过滤
               3.当self.filters为字典时:
                 3.1 如果row中的key不存在于self.filters.keys(),new_row[key] = row[key]
                 3.2 如果row中的key存在于self.filters.keys()中,
                     判断self.filters中的value是否可调用
                     如果可调用,则将row对应的value传入,
                        判断返回值,如果返回True,则new_row[key] = row[key]
                        否则new_row[key] = self.filters[key](row[key])
                     如果不可调用,则new_row[key] = self.filters[key]
               :param row: 需要处理的字典对象
               :return:
               """
        if not self.filters:
            return row
        if isinstance(self.filters, list):
            return {k: row.get(k, None) for k in self.filters}
        if isinstance(self.filters, dict):
            new_row = {}
            # 遍历所有的key值
            for key in set(row.keys()) | set(self.filters.keys()):
                if key in row.keys() and key not in self.filters.keys():
                    new_row[key] = row[key]
                elif key in self.filters.keys():
                    filter_value = self.filters[key]
                    # 针对filter_value不同的情况进行处理
                    if hasattr(filter_value, '__call__'):
                        value = filter_value(row, key)
                        # 返回True
                        if isinstance(value, bool) and value:
                            new_row[key] = row[key]
                        elif isinstance(value, bool) and not value:
                            pass
                        elif isinstance(value, tuple) and len(value) == 2:
                            new_row[value[0]] = value[1]
                        else:
                            raise CrawlRequestException(None, f"ERROR:{getLocalDate()}:无法进行转换")
                    else:
                        new_row[filter_value] = row.get(key, None)
                elif key not in row.keys() and key not in self.filters.keys():
                    pass
            return new_row


class CustomCrawlRequest(AbstractCrawlRequest):

    def __init__(self,
                 session: Session,
                 config: CrawlConfig,
                 check_props: List[str],
                 mask: str = None,
                 **kwargs):
        super().__init__()
        self.request: SessionRequest = SessionRequest()
        self.request.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "application/json;charset=UTF-8",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
        self.session = session
        self.processed_rows = []
        self.filters: List[RowFilter] = []
        self.check_props = check_props
        self.end_flag = False
        self.save_table = None
        self.sequence_name = None
        self.trigger_name = None
        self.mask = mask
        self.kwargs = kwargs
        self.crawl_config: CrawlConfig = config
        self.init_props(**kwargs)

    def init_props(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def add_filter(self, row_kv_filter: RowFilter):
        if issubclass(row_kv_filter.__class__, RowKVTransformAndFilter):
            self.filters.append(row_kv_filter)
        else:
            raise CrawlRequestException(None, f"EEEOR:{getLocalDate()}:add_filter需要添加RowFilter类实现类的Filter")

    def do_crawl(self):
        # 1.该方法仅能调用一次
        if getattr(self, 'do_crawl_exe_flag', None):
            # 如果为True则表示已经调用过
            raise CrawlRequestException(None, f"ERROR:{getLocalDate()}:do_crawl方法已经调用过,无法重复调用")
        # 2.执行爬虫数据需要进行的准备工作
        self._pre_crawl()
        logger.info("完成CustomCrawlRequest._pre_crawl准备工作")
        # 5.循环执行爬取数据的工作
        logger.info("开始循环爬取数据:")
        while not self.end_flag:
            self._config_params()
            logger.info("完成请求参数的配置:%s" % self.request)
            response = self._do_request()
            logger.info("完成请求,请求状态码:%s" % response.status_code)
            rows = self._parse_response(response)
            logger.info("完成响应解析,获取%s条数据" % len(rows))
            rows = self._filter_rows(rows)
            logger.info("完成数据过滤操作")
            rows = self._process_rows(rows)
            logger.info("完成数据处理操作")
            if rows:
                self.processed_rows += rows
            self._config_end_flag()
            logger.info("配置end_flag标识,当前end_flag:%s" % self.end_flag)
        logger.info("爬虫工作结束")
        # 标识该方法已经被调用过
        setattr(self, 'do_crawl_exe_flag', True)
        # 保存数据
        logger.info("开始保存数据")
        self.do_save()
        logger.info("成功保存数据")
        # 释放资源
        self.close()
        logger.info("成功释放资源")

    def _filter_rows(self, rows) -> List[dict]:
        count = 1
        for fil in self.filters:
            new_rows = []
            for row in rows:
                logger.info(f"filter-{count}处理{row}")
                new_row = fil.filter(row)
                logger.info(f"filter-{count}处理结果{new_row}")
                new_rows.append(new_row)
            count += 1
            rows = new_rows
        return new_rows

    @abstractmethod
    def _process_rows(self, rows: List[dict]) -> List[dict]:
        pass

    @abstractmethod
    def _pre_crawl(self):
        pass

    @abstractmethod
    def _config_params(self):
        pass

    def _do_request(self) -> Response:
        if isinstance(self.request, SessionRequest):
            response: Response = self.session.request(
                url=self.request.url,
                method=self.request.method,
                json=self.request.json,
                data=self.request.data,
                params=self.request.params)
        elif isinstance(self.request, dict):
            response: Response = self.session.request(
                **self.request)
        logger.info("请求响应码:%s" % response.status_code)
        return response

    @abstractmethod
    def _parse_response(self, response: Response) -> List[dict]:
        pass

    @abstractmethod
    def _config_end_flag(self):
        pass

    def do_save(self):
        conn = None
        cursor = None
        try:
            conn = self.crawl_config.db_pool.connection()
            cursor = conn.cursor()
            check_table = False
            for row in self.processed_rows:
                # 判断表是否存在
                if not check_table:
                    exist_flag = check_table_exists(self.save_table if self.save_table else get_table_name(self.mask),
                                                    cursor)
                    if not exist_flag:
                        create_table(row,
                                     'CLOB' if self.crawl_config.db_type.name == 'oracle' else 'text',
                                     'NUMBER(11)' if self.crawl_config.db_type.name == 'oracle' else 'long',
                                     self.save_table if self.save_table else get_table_name(self.mask),
                                     cursor,
                                     self.sequence_name if self.sequence_name else get_sequence_name(self.mask),
                                     self.trigger_name if self.trigger_name else get_trigger_name(self.mask)
                                     )
                        check_table = True
                # 执行插入操作
                self._save_one_row(row, cursor)
                logger.info(f"保存{row}")
            conn.commit()
            logger.info(f"提交保存数据事务")
        except Exception as e:
            if conn:
                logger.error(f"回滚保存数据事务")
                conn.rollback()
            raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _save_one_row(self, row, cursor):
        # 插入数据
        try:
            update_else_insert_to_db(
                cursor=cursor,
                target_table=self.save_table if self.save_table else get_table_name(self.mask),
                props_dict=row,
                check_props={k: row[k] for k in self.check_props},
                db_type=self.crawl_config.db_type
            )
        except Exception as e:
            exception = cast_exception(e)
            if isinstance(exception, CustomException):
                if exception.code == 1:
                    add_fields(cursor,
                               self.save_table if self.save_table else get_table_name(self.mask),
                               row.keys(),
                               'CLOB' if self.crawl_config.db_type.name == 'oracle' else 'text',
                               )
                    # 重新执行插入操作
                    self._save_one_row(
                        row,
                        cursor
                    )
            else:
                raise exception

    def close(self):
        if self.session:
            self.session.close()


class ConfigurableCrawlRequest(CustomCrawlRequest):
    def __init__(self,
                 log_id: int = None,
                 request: dict = None,
                 identifier: str = None,
                 field_name_2_new_field_name=None,
                 field_value_mapping=None,
                 mark_code_mapping_count: int = 1,
                 sleep_second=3,
                 **kwargs
                 ):
        super().__init__(
            session=requests.session(),
            config=crawl_config,
            check_props=['logId', 'cpbm'],
            mask=identifier
        )
        self.log_id = log_id
        self.request = request if request else {}
        self.mark_code_mapping = None
        self.mark_code_mapping_count = mark_code_mapping_count
        self.field_name_2_new_field_name = field_name_2_new_field_name
        self.field_value_mapping = field_value_mapping
        self.end_flag = False
        self.processed_rows = []
        self.sleep_second = sleep_second
        self.kwargs = kwargs

    def _process_rows(self, rows: List[dict]) -> List[dict]:
        new_rows = []
        for row in rows:
            new_row = self._row_transform(row)
            new_rows.append(new_row)
        return new_rows

    @abstractmethod
    def _pre_crawl(self):
        """
        请求之前的准备工作
        仅执行一次
        """
        pass

    @abstractmethod
    def _config_params(self):
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
    def _config_end_flag(self):
        """
        更新终止标识
        :param response:
        :return:
        """
        pass

    def _filter_rows(self, rows) -> List[dict]:
        return rows

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
        logger.debug(f'开始对key进行转换:{row}')
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
        logger.debug(f'value转换结果:{new_row}')
        return new_row

    def _row_value_mapping(self, row: dict, field_value_mapping: Dict[str, object] = None) -> dict:
        """
        对k/v中的v转换
        :param row:
        :param field_value_mapping:
        :return:
        """
        logger.debug(f'开始对value进行转换:{row}')
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
                        logger.debug(f"{k}:{str(v)}进行value转换,该value不存在于{value_mapping}中")
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
        logger.debug(f'value转换结果:{new_row}')
        return new_row

    @abstractmethod
    def _row_post_processor(self, row: dict):
        pass

    def _row_transform(self, row: dict):
        new_row = self._row_value_mapping(row, self.field_value_mapping)
        new_row = self._row_processor(new_row)
        new_row = self._row_key_mapping(new_row, self.field_name_2_new_field_name)
        new_row = self._row_post_processor(new_row)
        return new_row
