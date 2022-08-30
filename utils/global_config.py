from enum import Enum


class DBType(Enum):
    oracle = 'ORACLE'
    mysql = 'MYSQL'


ORACLE_CLIENT = r'D:\servers\instantclient_21_6'
LOG_TABLE = 'spider_log'
DB_ENV = DBType.oracle

mysql_host = '10.2.13.223'
mysql_user = 'root'
mysql_password = '123456'
mysql_database = 'hk_graph'
mysql_autocommit = False

oracle_user = 'stage'
oracle_password = 'User123$'
oracle_uri = '10.2.15.16:1521/testdb'


def init_oracle():
    import cx_Oracle as cx
    cx.init_oracle_client(lib_dir=ORACLE_CLIENT)
    return cx
