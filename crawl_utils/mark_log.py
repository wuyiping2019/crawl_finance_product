import datetime

from dbutils.pooled_db import PooledDB
from pymysql.cursors import Cursor
from global_config import LOG_TABLE
from db_utils import createInsertSql, update_to_db, get_conn, close


def getLocalDate():
    now = datetime.datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


def updateLogToDB(cur: Cursor, log_id: int, properties: dict, log_table=LOG_TABLE):
    """
    更新日志信息 没有提交事务(更新的属性除了properties中，自动包括updateTime字段)
    :param log_table:
    :param cur:
    :param log_id:
    :param properties:
    :return:
    """
    local_date = getLocalDate()
    properties['updateTime'] = local_date
    set_prop = ''
    for key in properties.keys():
        value = None
        if '"' in str(properties[key]):
            value = str(properties[key]).replace('"', '\\"')
        set_prop += "%s='%s' , " % (key, value if value is not None else properties[key])
    set_prop = set_prop.strip().strip(',')
    update_sql = """update %s set %s where id = %s """ % (log_table, set_prop, log_id)
    print(update_sql)
    cur.execute(update_sql)


def insertLogToDB(cur: Cursor, properties: dict, log_table=LOG_TABLE):
    """
    记录开始执行的日志信息 没有提交事务
    :param cur:
    :param properties:
    :param log_table:表名 默认写入日志表 可以使用该方法插入数据到其他表
    :return:返回插入的自增id
    """
    local_date = getLocalDate()
    properties['createTime'] = local_date
    fields, values = createInsertSql(properties)
    if '"' in values:
        values = values.replace('"', '\"')
    insert_sql = """insert into %s(%s) values(%s)""" % (log_table, fields, values)
    print(insert_sql)
    cur.execute(insert_sql)


def get_generated_log_id(name: str, db_poll: PooledDB):
    """
    :param log_table: 日志表
    :param name: 日志表中的name字段
    :param db_poll:
    :return:
    """
    # 查询的这条日志信息的自增主键id
    cursor, conn = None, None
    try:
        conn = db_poll.connection()
        cursor = conn.cursor()
        cursor.execute("select max(id) from %s where name = '%s'" % (LOG_TABLE, name))
        generated_log_id = cursor.fetchone()[0]
        conn.commit()
        return generated_log_id
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        close([cursor, conn])


def mark_start_log(name: str, start_date: str, db_poll: PooledDB = None):
    """
    记录开始日志
    :param name:
    :param start_date:
    :param db_poll:
    :return:
    """
    start_log = {
        "name": name,
        "startDate": start_date,
        "status": "正在执行中"
    }
    cursor, conn = None, None
    try:
        cursor = db_poll.connection().cursor()
        insertLogToDB(cur=cursor, properties=start_log)
        conn = cursor.connection
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        close([cursor, conn])


def mark_success_log(count: str, end_date: str, generated_log_id: int, db_poll: PooledDB = None):
    """
    记录成功日志
    :param count:
    :param end_date:
    :param generated_log_id:
    :param db_poll:
    :return:
    """
    success_log = {
        "status": "完成",
        "endDate": end_date,
        'updateTime': end_date,
        "count": count,
        "result": "成功",
        "detail": "成功"
    }
    conn, cursor = None, None
    try:
        conn = db_poll.connection()
        cursor = conn.cursor()
        update_to_db(cursor=cursor,
                     update_props=success_log,
                     constraint_props={'id': generated_log_id},
                     target_table=LOG_TABLE)
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        close([cursor, conn])


def mark_failure_log(e: Exception, endDate: str, generated_log_id: int, db_poll: PooledDB, count):
    """
    记录失败日志
    :param count:
    :param e:
    :param endDate:
    :param generated_log_id:
    :param db_poll:
    :return:
    """
    log_info = {
        "status": "完成",
        "endDate": endDate,
        'updateTime': getLocalDate(),
        "count": count,
        "result": "失败",
        "detail": str(e)
    }
    conn, cursor = None, None
    try:
        conn = db_poll.connection()
        cursor = conn.cursor()
        update_to_db(cursor=cursor,
                     update_props=log_info,
                     constraint_props={'id': generated_log_id},
                     target_table=LOG_TABLE)
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        close([cursor, conn])


def get_write_count(table_name: str, generated_log_id: int, db_poll: PooledDB):
    """
    查询爬取的数据写入到指定表的数据条数
    :param table_name:
    :param generated_log_id:
    :param db_poll:
    :return:
    """
    conn, cursor = None, None
    try:
        conn = db_poll.connection()
        cursor = conn.cursor()
        cursor.execute("select count(1) as count from %s where logId=%s" % (table_name, generated_log_id))
        rs = cursor.fetchone()[0]
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        close([cursor, conn])
    return rs
