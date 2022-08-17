import datetime

from pymysql.cursors import Cursor
from global_config import LOG_TABLE
from db_utils import createInsertSql


def getLocalDate():
    now = datetime.datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


def updateLogToDB(cur: Cursor, log_id: int, properties: dict):
    """
    更新日志信息 没有提交事务(更新的属性除了properties中，自动包括updateTime字段)
    :param cur:
    :param log_id:
    :param properties:
    :return:
    """
    local_date = getLocalDate()
    properties['updateTime'] = local_date
    set_prop = ''
    for key in properties.keys():
        set_prop += "%s='%s' , " % (key, properties[key])
    set_prop = set_prop.strip().strip(',')
    update_sql = """update %s set %s where id = %s """ % (LOG_TABLE, set_prop, log_id)
    cur.execute(update_sql)
    cur.connection.commit()


def insertLogToDB(cur: Cursor, properties: dict):
    """
    记录开始执行的日志信息 没有提交事务
    :param cur:
    :param properties:
    :return:返回插入的自增id
    """
    local_date = getLocalDate()
    properties['createTime'] = local_date
    fields, values = createInsertSql(properties)
    insert_sql = """insert into %s(%s) values(%s)""" % (LOG_TABLE, fields, values)
    cur.execute(insert_sql)
    cur.connection.commit()
