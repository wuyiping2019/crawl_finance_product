from __future__ import annotations

import abc
import types
from inspect import isfunction

import requests
from requests import Session

from config_parser import CrawlConfig
from crawl import MutiThreadCrawl
from crawl_utils.db_utils import close
from crawl_utils.global_config import DB_ENV, init_oracle
from crawl_utils.mark_log import mark_start_log, mark_failure_log, getLocalDate, get_generated_log_id, mark_success_log, \
    get_write_count

if DB_ENV == 'ORACLE':
    init_oracle()


class SpiderFlow:
    @abc.abstractmethod
    def callback(self, session: Session, log_id: int, config: CrawlConfig, **kwargs):
        """
               实现具体处理数据的过程
               :param config:
               :param conn: 数据库连接的Connection对象
               :param cursor: conn.cursor()获取的指针对象
               :param session: requests.session()获取的session对象
               :param log_id: 本次爬取数据过程在日志表中生成的一条反应爬取过程的日志数据的唯一id
               :param muti_thread_crawl
               :param kwargs: 额外需要的参数
               :return:
               """
        pass


def process_flow(log_name: str,
                 target_table: str,
                 callback: types.FunctionType | types.LambdaType | SpiderFlow,
                 config: CrawlConfig,
                 **kwargs):
    conn = None
    cursor = None
    session = None
    generated_log_id = None
    count = 0
    try:
        session = requests.session()
        # 记录开始日志
        mark_start_log(log_name, getLocalDate(), config.db_pool)
        # 获取日志id
        generated_log_id = get_generated_log_id(log_name, config.db_pool)
        # 处理爬取过程
        """
        1.传入的callback可以是一个定义的普通方法，该方法至少传入conn、cursor、session、log_id参数，其他参数以键值对传入**kwargs
        2.传入的callback是一个SpiderFlow对象，此时调用该对象的callback方法
        """
        if isinstance(callback, SpiderFlow):
            callback.callback(session=session,
                              log_id=generated_log_id,
                              config=config,
                              **kwargs)
        if isfunction(callback):
            callback(session=session,
                     log_id=generated_log_id,
                     config=config,
                     **kwargs)
        # 查询插入的数据条数
        count = get_write_count(target_table, generated_log_id, config.db_pool)
        # 记录成功日志
        mark_success_log(count, getLocalDate(), generated_log_id, config.db_pool)
    except Exception as e:
        # 记录失败日志
        if generated_log_id:
            mark_failure_log(e, getLocalDate(), generated_log_id, config.db_pool, count)
        raise e
    finally:
        # 释放资源
        close([cursor, conn, session])


if __name__ == '__main__':
    """
    测试
    """


    def test(a, b, c):
        return a + b + c


    process_flow(log_name='1', callback=test, a=2, b=2, c=2)