import pymysql
from global_config import init_oracle
from global_config import mysql_database, mysql_password, mysql_user, mysql_autocommit, mysql_host
from global_config import oracle_user, oracle_password, oracle_uri
import cx_Oracle as cx


def createInsertSql(properties: dict):
    """
        Return
            Fields,like `key1,key2,key3`
            Values,like `'value1','value2','value3'`
        used for create insert sql,like 'insert into tabName(%s) values(%s)'%(Fields,Values)
        :properties props: 字符串属性值的字典
        :return:
        """
    fields = ''
    values = ''
    for key in properties.keys():
        value = properties[key]
        fields += '%s,' % key
        values += "'%s'," % value
    fields = fields.strip(',')
    values = values.strip(',')
    return fields, values


def get_conn_mysql():
    connect = pymysql.connect(host=mysql_host,
                              user=mysql_user,
                              password=mysql_password,
                              database=mysql_database)
    connect.autocommit(mysql_autocommit)
    return connect


def get_conn_oracle():
    return cx.connect(oracle_user, oracle_password, oracle_uri)


def close(objs: list):
    for obj in objs:
        if obj:
            obj.close()


__all__ = [
    "get_conn_mysql", "get_conn_oracle", "createInsertSql", "close"
]
