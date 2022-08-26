import abc
from inspect import isfunction

import requests
from requests import Session

from utils.db_utils import get_conn_oracle, close
from utils.global_config import DB_ENV, init_oracle
from utils.mark_log import mark_start_log, mark_failure_log, getLocalDate, get_generated_log_id, mark_success_log, \
    get_write_count

if DB_ENV == 'ORACLE':
    init_oracle()


class SpiderFlow:
    @abc.abstractmethod
    def callback(self, conn, cursor, session: Session, log_id: int, **kwargs):
        """
               实现具体处理数据的过程
               :param conn: 数据库连接的Connection对象
               :param cursor: conn.cursor()获取的指针对象
               :param session: requests.session()获取的session对象
               :param log_id: 本次爬取数据过程在日志表中生成的一条反应爬取过程的日志数据的唯一id
               :param kwargs: 额外需要的参数
               :return:
               """
        pass


def process_flow(log_name, target_table, callback, **kwargs):
    conn = None
    cursor = None
    session = None
    generated_log_id = None
    try:
        conn = get_conn_oracle()
        cursor = conn.cursor()
        session = requests.session()
        # 记录开始日志
        mark_start_log(log_name, getLocalDate(), cursor)
        # 获取日志id
        generated_log_id = get_generated_log_id(log_name, cursor)
        # 处理爬取过程
        """
        1.传入的callback可以是一个定义的普通方法，该方法至少传入conn、cursor、session、log_id参数，其他参数以键值对传入**kwargs
        2.传入的callback是一个SpiderFlow对象，此时调用该对象的callback方法
        """
        if isinstance(callback, SpiderFlow):
            callback.callback(conn=conn, cursor=cursor, session=session, log_id=generated_log_id, **kwargs)
        if isfunction(callback):
            callback(conn=conn,
                     cursor=cursor,
                     session=session,
                     log_id=generated_log_id,
                     **kwargs)
        # 查询插入的数据条数
        count = get_write_count(target_table, generated_log_id, cursor)
        print("插入的数据条数"+str(count))
        # 记录成功日志
        mark_success_log(count, getLocalDate(), generated_log_id, cursor)
    except Exception as e:
        # 记录失败日志
        if generated_log_id:
            mark_failure_log(e, getLocalDate(), generated_log_id, cursor)
        # 回滚事务
        if cursor:
            cursor.connection.rollback()
        print(e)
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
