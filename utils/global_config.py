from enum import Enum


class DBType(Enum):
    oracle = 'ORACLE'
    mysql = 'MYSQL'


ORACLE_CLIENT = r'D:\servers\instantclient_21_6'
ORACLE_CLIENT = r'E:\pycharm_projects\instantclient_21_6'
LOG_TABLE = 'spider_log'
DB_ENV = DBType.oracle

mysql_host = '10.2.13.223'
mysql_user = 'root'
mysql_password = '123456'
mysql_database = 'hk_graph'
mysql_autocommit = False
mysql_str_type = 'varchar(1000)'
mysql_number_type = 'int(11)'

oracle_user = 'stage'
oracle_password = 'User123$'
oracle_uri = '10.2.15.16:1521/testdb'
oracle_str_type = 'varchar2(1000)'
oracle_number_type = 'number(11)'

oracle_user = 'User123'
oracle_password = '123456'
oracle_uri = '127.0.0.1:1521/orcl'
oracle_str_type = 'varchar2(1000)'
oracle_number_type = 'number(11)'


def init_oracle(lib_dir=None):
    import cx_Oracle as cx
    if not lib_dir:
        lib_dir = ORACLE_CLIENT
    try:
        cx.init_oracle_client(lib_dir=lib_dir)
    except Exception as e:
        pass
    return cx


def get_field_type():
    if DB_ENV == DBType.oracle:
        return oracle_str_type, oracle_number_type
    if DB_ENV == DBType.mysql:
        return mysql_str_type, mysql_number_type


def get_table_name(mask: str):
    return f'ip_bank_{mask}_processed'


def get_trigger_name(mask: str):
    return f'trigger_{mask}'


def get_sequence_name(mask: str):
    return f'seq_{mask}'
