from __future__ import annotations
import time
from abc import abstractmethod
from enum import Enum
from typing import List, Dict
import requests
from requests import Response, Session
from crawl_utils.common_utils import delete_empty_value
from crawl_utils.custom_exception import cast_exception, CustomException
from crawl_utils.db_utils import check_table_exists, create_table, update_else_insert_to_db, add_fields
from crawl_utils.global_config import get_table_name, get_sequence_name, get_trigger_name
from crawl_utils.logging_utils import get_logger
from config_parser import CrawlConfig

logger = get_logger(name=__name__)

simple_logger = get_logger(name='simple' + __name__, formatter='%(asctime)s %(message)s')


class CrawlRequestException(Exception):
    """"
    code及其对应msg信息在CrawlRequestExceptionEnum中
    """

    def __init__(self, code, msg, **kwargs):
        """
        :param code:错误唯一
        :param msg: 错误信息
        """
        self.code = code
        self.msg = msg
        self.kwargs = kwargs


def do_crawl_exception(method):
    return lambda crawl_type, crawl_name: f'{crawl_type}-{crawl_name}.{method}异常'


def raise_muti_crawl_request_exception(errors: List[Exception]):
    raise CrawlRequestException(
        code=CrawlRequestExceptionEnum.MUTI_CRAWL_REQUEST_EXCEPTION.code,
        msg=CrawlRequestExceptionEnum.MUTI_CRAWL_REQUEST_EXCEPTION.msg,
        error=errors
    )


def raise_crawl_request_exception(errors: List[Exception]):
    if not errors:
        return
    elif len(errors) >= 2:
        raise_muti_crawl_request_exception(errors)
    elif len(errors) == 1:
        raise errors[0]
    else:
        pass


class CrawlRequestExceptionEnum(Enum):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

    TABLE_NULL_EXCEPTION = 1, '无法获取需要将数据保存的表:mask和save_table不能同时为空'
    CHECK_PROPS_NULL_EXCEPTION = 2, '执行更新否则插入操作,需要指定判断一条数据在表中唯一标识的字段列表'
    RECALLING_DO_CRAWL_EXCEPTION = 3, '重复调用do_crawl方法异常'
    CRAWL_FAILED_EXCEPTION = 4, '爬取银行理财产品失败'
    LOG_ID_ATTR_MISSING = 5, '保存的数据字典中不存在logId的key值'
    SAVE_TABLE_NOT_EXISTS_EXCEPTION = 6, '目标表不存在'
    INSERT_ROW_EXCEPTION = 7, '插入数据异常'
    CRAWL_REQUEST_PRE_CRAWL_EXCEPTION = 8, do_crawl_exception('_pre_crawl')
    CRAWL_REQUEST_CONFIG_PARAMS_EXCEPTION = 9, do_crawl_exception('_config_params')
    CRAWL_REQUEST_DO_REQUEST_EXCEPTION = 10, do_crawl_exception('_do_request')
    CRAWL_REQUEST_PARSE_RESPONSE_EXCEPTION = 11, do_crawl_exception('_parse_response')
    CRAWL_REQUEST_FILTER_ROWS_EXCEPTION = 12, do_crawl_exception('_filter_rows')
    CRAWL_REQUEST_CONFIG_END_FLAG_EXCEPTION = 13, do_crawl_exception('_config_end_flag')
    CRAWL_REQUEST_DO_SAVE_EXCEPTION = 14, do_crawl_exception('do_save')
    MUTI_CRAWL_REQUEST_EXCEPTION = 15, '捕获多个异常'


class AbstractCrawlRequest:
    """
    定义爬取数据工作流的抽象类:类似java中的接口 仅提供一个do_crawl方法 全部的爬取数据流逻辑都必须在do_crawl方法中实现
    """

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
    """
    2022-10-13:扩展增加name属性
    为了不影响原代码:使用@property注释增加name方法
    """

    @property
    def name(self):
        return getattr(self, '_name', '')

    def set_name(self, name):
        setattr(self, '_name', name)
        return self

    @abstractmethod
    def filter(self, row: dict) -> dict:
        pass

    def __repr__(self):
        return f"RowFilter(name={self.name})"


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
        # 当过滤器为空时 直接返回
        if not self.filters:
            return row
        # 当过滤器是列表时 表示过滤字典
        elif isinstance(self.filters, list):
            return {k: row.get(k, None) for k in self.filters}
        # 当过滤器是字典时
        elif isinstance(self.filters, dict):
            new_row = {}
            # 遍历所有的key值
            for key in set(row.keys()) | set(self.filters.keys()):
                filter_value = self.filters.get(key, None)
                if filter_value is None:
                    new_row[key] = row.get(key, None)
                elif hasattr(filter_value, '__call__'):
                    value = filter_value(row, key)
                    if isinstance(value, tuple) and len(value) == 2:
                        new_row[value[0]] = value[1]
                    else:
                        new_row[key] = value
                elif isinstance(filter_value, dict):
                    # 表示对row中key的value进行转码
                    new_row[key] = filter_value.get(row.get(key, None), row.get(key, None))
                else:
                    # 直接将赋值
                    new_row[filter_value] = row.get(key, None)
            return new_row

    def __repr__(self):
        return f"RowKVTransformAndFilter({self.name})"


class CustomCrawlRequest(AbstractCrawlRequest):
    """
    定义一个常常使用requests.session对象进行数据爬取的抽象类 继承AbstractCrawlRequest类
    """

    def __init__(self,
                 name: str,
                 session: Session,
                 config: CrawlConfig,
                 check_props: List[str],
                 mask: str = None
                 ):
        super().__init__()
        self.name = name
        self.request: SessionRequest = SessionRequest()
        self.default_request_headers = {
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
        # requests.session()返回的Session对象
        self._session = session
        # 一个配置对象,该对象含有各种配置项,如日志级别、数据库连接池等,该对象中的需要释放资源的对象由创建者关闭
        self.crawl_config: CrawlConfig = config
        # 存储爬取过程中最后处理之后返回的数据
        self.processed_rows = []
        # 过滤器链：_parse_response返回的数据经过该过滤器链处理之后会添加到processed_rows列表中
        self.filters: List[RowFilter] = []
        self.check_props = check_props
        self.mask = mask
        self.end_flag = False
        # 默认在保存数据时优先使用save_table属性 当该属性为空时 使用get_table_name(mask)方法返回的表名保存数据
        self.save_table = None
        # 默认如果保存数据使用的是oracle数据库并且不存在表时 会使用序列和触发器创建一个含有自增id主键的表 有限使用sequence_name和trigger_name为触发器名称和触发器名称
        # 如果需要创建序列和触发器且sequence_name和trigger_name为空时,会使用get_sequence_name(mask)方法和get_trigger_name(mask)方法获取序列和触发器名称
        self.sequence_name = None
        self.trigger_name = None
        self.candidate_check_props = {}

    def init_props(self, **kwargs):
        simple_logger.info(f"开始爬取《{self.name}》的数据...")
        logger.info(f"正在初始化{type(self).__name__}对象...")
        for k, v in kwargs.items():
            setattr(self, k, v)
        logger.info(f"成功初始化{type(self).__name__}对象")
        return self

    # __init__构造器中使用的是crawl_config属性,额外增加config属性等价于crawl_config属性
    @property
    def config(self):
        return self.crawl_config

    @config.setter
    def config(self, value):
        self.crawl_config = value

    @property
    def session(self) -> Session:
        """
        对外的session属性:避免直接使用_session属性
        :return:
        """
        return self._session

    @session.setter
    def session(self, session):
        """
        设置session属性:实际上设置的是_session属性,避免在_session非空的情况下,对_session重新赋新的Session对象没有释放资源
        :param session:
        :return:
        """
        if self._session and hasattr(self._session, '__call__'):
            self._session.close()
        self._session = session

    def add_filter(self, filter: RowFilter):
        """
        向空过滤器链中添加过滤器
        :param filter:
        :return:
        """
        if isinstance(filter, RowFilter):
            self.filters.append(filter)
        else:
            logger.error(f"向CustomCrawlRequest过滤器链中添加的过滤器必须为RowFilter类及其子类对象")
            raise CrawlRequestException(None, f"无法添加非RowFilter类型的过滤器")

    def do_crawl(self, **kwargs):
        try:
            self._do_crawl()
            logger.info(f"{self.name}-成功获取数据")
        except Exception as e:
            # 此处抛出的异常全部都是CrawlRequestException类型的异常
            raise e
        finally:
            # 释放资源
            self.close()
            logger.info(f"{self.name}-成功释放资源")

    def _do_crawl(self):
        """
        为了避免大块的try except块 close方法放在do_crawl方法中
        核心功能类：完成爬虫工作流
        1.爬虫之前的准备工作 调用_pre_crawl方法
        2.开始循环:判断end_flag标识
        3.配置参数 调用_config_params方法
        4.执行请求 调用_do_request方法
        5.解析响应 调用_parse_response方法
        6.处理数据 调用_filter_rows方法
        7.终止标识 判断end_flag
        8.end_flag为True继续循环 返回第3步 否则终止循环
        9.保存数据 调用do_save方法
        10.释放资源 调用close方法
        :return:
        """
        # 1.该方法仅能调用一次
        if getattr(self, 'do_crawl_exe_flag', None):
            # 如果为True则表示已经调用过
            raise CrawlRequestException(
                CrawlRequestExceptionEnum.RECALLING_DO_CRAWL_EXCEPTION.code,
                CrawlRequestExceptionEnum.RECALLING_DO_CRAWL_EXCEPTION.msg
            )
        # 2.执行爬虫数据需要进行的准备工作
        try:
            self._pre_crawl()
            logger.info(f"完成{self.name}._pre_crawl准备工作")
        except Exception as e:
            raise CrawlRequestException(
                code=CrawlRequestExceptionEnum.CRAWL_REQUEST_PRE_CRAWL_EXCEPTION.code,
                msg=CrawlRequestExceptionEnum.CRAWL_REQUEST_PRE_CRAWL_EXCEPTION.msg(type(self).__name__, self.name),
                error=e
            )
        # 5.循环执行爬取数据的工作
        logger.info(f"开始循环爬取{self.name}数据:")
        while not self.end_flag:
            logger.info(f"开始执行《{self.name}._config_params》,设置请求参数")
            try:
                self._config_params()
            except Exception as e:
                raise CrawlRequestException(
                    code=CrawlRequestExceptionEnum.CRAWL_REQUEST_CONFIG_PARAMS_EXCEPTION.code,
                    msg=CrawlRequestExceptionEnum.CRAWL_REQUEST_CONFIG_PARAMS_EXCEPTION.msg(type(self).__name__,
                                                                                            self.name),
                    error=e
                )
            if isinstance(self.request, dict):
                logger.info(f"完成{self.name}请求参数的配置:")
                logger.info(f"{self.name}-请求url:%s" % self.request.get('url', ''))
                logger.info(f"{self.name}-method:%s" % self.request.get('method', ''))
                if self.request.get('data', ''):
                    logger.info(f"{self.name}-请求data:%s" % self.request.get('data', ''))
                if self.request.get('json', ''):
                    logger.info(f"{self.name}-请求json:%s" % self.request.get('json', ''))
                if self.request.get('params', ''):
                    logger.info(f"{self.name}-请求params:%s" % self.request.get('params', ''))
            try:
                response = self._do_request()
            except Exception as e:
                raise CrawlRequestException(
                    code=CrawlRequestExceptionEnum.CRAWL_REQUEST_DO_REQUEST_EXCEPTION.code,
                    msg=CrawlRequestExceptionEnum.CRAWL_REQUEST_DO_REQUEST_EXCEPTION.msg(type(self).__name__,
                                                                                         self.name),
                    error=e
                )
            time.sleep(1)
            if response.status_code == 200:
                logger.info(f"{self.name}-成功获取响应结果,响应状态码:%s" % response.status_code)
            else:
                logger.warn(f"{self.name}-无法获取响应,响应状态码:%s" % response.status_code)
                continue
            logger.info(f"{self.name}-开始解析响应结果...")
            try:
                rows = self._parse_response(response)
            except Exception as e:
                raise CrawlRequestException(
                    code=CrawlRequestExceptionEnum.CRAWL_REQUEST_PARSE_RESPONSE_EXCEPTION.code,
                    msg=CrawlRequestExceptionEnum.CRAWL_REQUEST_PARSE_RESPONSE_EXCEPTION.msg(type(self).__name__,
                                                                                             self.name),
                    error=e
                )
            logger.info(f"{self.name}-完成响应解析,获取%s条数据" % len(rows))
            logger.info(f"{self.name}-开始使用过滤器链:%s,处理数据" % self.filters)
            try:
                rows = self._filter_rows(rows)
            except Exception as e:
                raise CrawlRequestException(
                    code=CrawlRequestExceptionEnum.CRAWL_REQUEST_FILTER_ROWS_EXCEPTION.code,
                    msg=CrawlRequestExceptionEnum.CRAWL_REQUEST_FILTER_ROWS_EXCEPTION.msg(type(self).__name__,
                                                                                          self.name),
                    error=e
                )
            logger.info(f"完成{self.name}本次请求的数据过滤操作")
            if rows:
                self.processed_rows += rows
            logger.info(f"{self.name}-开始设置{type(self).__name__}的end_flag标识")
            try:
                self._config_end_flag()
            except Exception as e:
                raise CrawlRequestException(
                    code=CrawlRequestExceptionEnum.CRAWL_REQUEST_CONFIG_END_FLAG_EXCEPTION.code,
                    msg=CrawlRequestExceptionEnum.CRAWL_REQUEST_CONFIG_END_FLAG_EXCEPTION.msg(type(self).__name__,
                                                                                              self.name),
                    error=e
                )
            logger.info(f"{self.name}-当前{type(self).__name__}的end_flag:%s,%s" %
                        (self.end_flag, '继续爬取数据' if self.end_flag else '终止数据爬取'))
        logger.info(f"{self.name}-爬虫工作结束")
        # 标识该方法已经被调用过
        setattr(self, 'do_crawl_exe_flag', True)
        # 保存数据
        logger.info(f"{self.name}-开始保存数据")
        try:
            self.do_save()
        except Exception as e:
            raise CrawlRequestException(
                code=CrawlRequestExceptionEnum.CRAWL_REQUEST_DO_SAVE_EXCEPTION.code,
                msg=CrawlRequestExceptionEnum.CRAWL_REQUEST_DO_SAVE_EXCEPTION.msg(type(self).__name__, self.name),
                error=e

            )
        logger.info(f"{self.name}-成功保存数据")

    def _filter_rows(self, rows: List[dict]) -> List[dict]:
        """
        逐条数据经过过滤器链进行处理
        :param rows:
        :return:
        """
        count = 1
        new_rows = []
        for fil in self.filters:
            new_rows = []
            if not fil:
                logger.debug(f"{self.name}-{type(self).__name__}-filter-{fil.name}-{count}-为空")
                count += 1
                continue
            for row in rows:
                logger.debug(f"{self.name}-{type(self).__name__}-filter-{fil.name}-{count}-处理:{row}")
                new_row = fil.filter(row)
                logger.debug(f"{self.name}-{type(self).__name__}-filter-{fil.name}-{count}-处理结果:{new_row}")
                new_rows.append(new_row)
            count += 1
            rows = new_rows
        return new_rows

    @abstractmethod
    def _pre_crawl(self):
        pass

    @abstractmethod
    def _config_params(self):
        pass

    def _do_request(self) -> Response:
        """
        使用requests.session().request()方法获取响应结果
        :return:
        """
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
        else:
            raise CrawlRequestException(None, "调用_do_request失败,request必须为SessionRequest对象或一个字典对象")
        return response

    @abstractmethod
    def _parse_response(self, response: Response) -> List[dict]:
        pass

    @abstractmethod
    def _config_end_flag(self):
        pass

    def do_save(self):
        # 检测mask和save_table不能同时为空
        if not self.mask and not self.save_table:
            raise CrawlRequestException(
                CrawlRequestExceptionEnum.TABLE_NULL_EXCEPTION.code,
                CrawlRequestExceptionEnum.TABLE_NULL_EXCEPTION.msg)
        if not self.check_props:
            raise CrawlRequestException(
                CrawlRequestExceptionEnum.CHECK_PROPS_NULL_EXCEPTION.code,
                CrawlRequestExceptionEnum.CHECK_PROPS_NULL_EXCEPTION.msg
            )
        conn = None
        cursor = None
        if not self.processed_rows:
            return
        try:
            conn = self.crawl_config.db_pool.connection()
            cursor = conn.cursor()
            self.save_table = self.save_table if self.save_table else get_table_name(self.mask)
            if not self.save_table:
                raise CrawlRequestException(
                    CrawlRequestExceptionEnum.TABLE_NULL_EXCEPTION.code,
                    CrawlRequestExceptionEnum.TABLE_NULL_EXCEPTION.msg
                )
            logger.info(f"{self.name}开始检测目标表:%s,是否存在" % self.save_table)
            table_exists_flag = check_table_exists(self.save_table, self.config.db_pool)
            if table_exists_flag:
                logger.info(f"{self.name}-检测到{self.save_table}存在")
            else:
                logger.warning(f"{self.name}-检测到{self.save_table}不存在")
                logger.info(f"{self.name}-开始自动创建{self.save_table}")
                create_table(self.processed_rows[0],
                             'CLOB' if self.crawl_config.db_type.name == 'oracle' else 'text',
                             'NUMBER(11)' if self.crawl_config.db_type.name == 'oracle' else 'long',
                             self.save_table if self.save_table else get_table_name(self.mask),
                             cursor,
                             self.sequence_name if self.sequence_name else get_sequence_name(self.mask),
                             self.trigger_name if self.trigger_name else get_trigger_name(self.mask)
                             )
            for row in self.processed_rows:
                logger.debug(f'{self.name}-正在保存{row}')
                # 执行插入操作
                self._save_one_row(row, cursor)
                logger.debug(f"{self.name}-成功保存{row}")
            conn.commit()
            logger.info(f"{self.name}-提交事务")
        except Exception as e:
            if conn:
                conn.rollback()
                logger.error(f"{self.name}-回滚事务")
            raise e
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def _save_one_row(self, row, cursor):
        if not row:
            return
        delete_empty_value(row)
        if not row:
            return
        # 插入数据
        try:
            update_else_insert_to_db(
                cursor=cursor,
                target_table=self.save_table if self.save_table else get_table_name(self.mask),
                props_dict=row,
                check_props={k: row[k] if k in row.keys() else row[self.candidate_check_props[k]] for k in
                             self.check_props},
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
            else:
                raise exception

    def close(self):
        if self.session:
            self.session.close()


class ConfigurableCrawlRequest(CustomCrawlRequest):
    def __init__(self,
                 name: str,
                 log_id: int = None,
                 request: dict = None,
                 identifier: str = None,
                 field_name_2_new_field_name: dict = None,
                 field_value_mapping=None,
                 mark_code_mapping_count: int = 1,
                 sleep_second=3,
                 **kwargs
                 ):
        super().__init__(
            name=name,
            session=requests.session(),
            config=CrawlConfig(),
            check_props=['logId', 'cpbm'],
            mask=identifier
        )
        self.log_id = log_id
        self.request = request if request else {}
        # 创建一个具有5个空过滤器的过滤器链
        self.filters = [None, None, None, None, None]

        self.mark_code_mapping = None
        self.mark_code_mapping_count = mark_code_mapping_count

        self._field_name_2_new_field_name = field_name_2_new_field_name
        self.field_name_2_new_field_name = self._field_name_2_new_field_name

        self._field_value_mapping = field_value_mapping
        self.field_value_mapping = self._field_value_mapping

        self.end_flag = False
        self.processed_rows = []
        self.sleep_second = sleep_second
        self.kwargs = kwargs
        # 该过滤器处于过滤器链中的第1个 处于过滤器链中的头
        if self._row_processor.__dict__.get('__isabstractmethod__', False):
            # 当_row_processor没有被实现的话
            pass
        else:
            # 当子类实现了_row_processor时
            row_filter = RowFilter().set_name('_row_processor:按实现的该抽象方法处理row')
            row_filter.filter = self._row_processor
            self.filters[0] = row_filter
        # 该过滤器处于过滤器链中的第5个 处于过滤器链中的尾部
        if self._row_post_processor.__dict__.get('__isabstractmethod__', False):
            pass
        else:
            # 当子类实现了_row_processor时
            row_post_filter = RowFilter().set_name('_row_post_processor:按实现的该抽象方法处理row')
            row_post_filter.filter = self._row_post_processor
            self.filters[4] = row_post_filter

    def add_filter_after_row_processor(self, filter: RowFilter):
        self.filters.insert(1, filter)

    def add_filter_after_filter_by_name(self, add_filter: RowFilter, after_filter_name: str):
        find_index = 0
        for index, filter in enumerate(self.filters):
            if filter.name == after_filter_name:
                find_index = index
                break
        if find_index:
            if find_index + 1 == len(self.filters):
                # 表示需要向filter过滤器链最后面增加一个过滤器
                self.filters.append(add_filter)
            else:
                self.filters.insert(find_index + 1, add_filter)

    @property
    def field_name_2_new_field_name(self):
        return self._field_name_2_new_field_name

    @field_name_2_new_field_name.setter
    def field_name_2_new_field_name(self, value):
        # 设置该属性会重置过滤器链中第3个和第4个过滤器
        if value:
            self._field_name_2_new_field_name = value
            self.filters[2] = RowKVTransformAndFilter(value).set_name(
                'field_name_2_new_field_name:使用该字典的key/value映射转换row中的key值')
            self.filters[3] = RowKVTransformAndFilter(list(value.values())).set_name(
                'field_name_2_new_field_name:使用该字典的value值过滤row中的key值')

    @property
    def field_value_mapping(self):
        return self._field_value_mapping

    @field_value_mapping.setter
    def field_value_mapping(self, value):
        # 设置该属性会重置过滤器链中第2个过滤器
        self.filters[1] = RowKVTransformAndFilter(value if value is None else {}).set_name(
            "FIELD_VALUE_MAPPING:按字典映射配置的字段处理方式处理row中的字段")

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

    @abstractmethod
    def _row_processor(self, row: dict) -> dict:
        """
        过滤器链中的第一个过滤器
        :param row:
        :return:
        """
        pass

    @abstractmethod
    def _row_post_processor(self, row: dict) -> dict:
        """
        过滤器链中的第五个过滤器
        :param row:
        :return:
        """
        pass

    @property
    def url(self):
        return self.request['url']

    @url.setter
    def url(self, value):
        self.request['url'] = value

    @property
    def method(self):
        return self.request['method']

    @method.setter
    def method(self, value):
        self.request['method'] = value

    @property
    def data(self):
        return self.request['data']

    @data.setter
    def data(self, value):
        self.request['data'] = value

    @property
    def json(self):
        return self.request['json']

    @json.setter
    def json(self, value):
        self.request['json'] = value

    @property
    def params(self):
        return self.request['params']

    @params.setter
    def params(self, value):
        self.request['params'] = value

    @property
    def headers(self):
        return self.request['headers']

    @headers.setter
    def headers(self, value):
        self.request['headers'] = value
