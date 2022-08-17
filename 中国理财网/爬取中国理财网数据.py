import datetime
import json
import requests
from pymysql.cursors import Cursor
from utils.global_config import LOG_TABLE, init_oracle, DB_ENV
from utils.db_utils import createInsertSql, get_conn_oracle
from utils.mark_log import insertLogToDB,updateLogToDB

NAME = '中国理财网站'
ZGLCW_BANK_TABLE = 'ip_zglcw_bank'
# 初始化连接oracle本地客户端
if DB_ENV == 'ORACLE':
    init_oracle()


def getLocalDate():
    now = datetime.datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")


def writeDataToDB(cur: Cursor, properties: dict):
    """
    按properties的键值对插入到数据库中,不提交事务
    :param cur:
    :param properties:
    :return:
    """
    local_date = getLocalDate()
    properties['createTime'] = local_date
    fields, values = createInsertSql(properties)
    insert_sql = """insert into %s(%s) values(%s)""" % (ZGLCW_BANK_TABLE, fields, values)
    cur.execute(insert_sql)
    cur.connection.commit()


if __name__ == '__main__':
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Length': '6',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'www.chinawealth.com.cn',
        'Origin': 'https://www.chinawealth.com.cn',
        'Pragma': 'no-cache',
        'Referer': 'https://www.chinawealth.com.cn/zzlc/jsp/lccp.jsp',
        'sec-ch-ua': '"Google Chrome";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    conn = None
    cursor = None
    generated_log_id = None
    session = None
    try:
        # 获取数据库连结对象
        conn = get_conn_oracle()
        cursor = conn.cursor()
        # 记录开始日志
        start_log = {
            "name": NAME,
            "startDate": getLocalDate(),
            "status": "正在执行中"
        }
        insertLogToDB(cur=cursor, properties=start_log)

        # 查询的这条日志信息的自增主键id
        cursor.execute("select max(id) from %s where name = '%s'" % (LOG_TABLE, NAME))
        generated_log_id = cursor.fetchone()[0]
        # 获取银行信息 写入zglcw_bank表
        session = requests.session()
        data = {
            'code': 0
        }
        resp = session.post('https://www.chinawealth.com.cn/dmmsQuery.go', data=data, headers=headers).text
        json_obj = json.loads(resp)
        for item in json_obj:
            # 添加
            item['logId'] = generated_log_id
            writeDataToDB(cur=cursor, properties=item)
        # 统一提交事务
        conn.commit()
        # 记录成功日志
        cursor.execute("select count(1) as count from %s where logId=%s" % (ZGLCW_BANK_TABLE, generated_log_id))
        count = cursor.fetchone()[0]
        success_log = {
            "status": "完成",
            "endDate": getLocalDate(),
            "count": count,
            "result": "成功",
            "detail": "成功"
        }
        updateLogToDB(cur=cursor, log_id=generated_log_id, properties=success_log)
        conn.commit()
    # 记录成功日志
    except Exception as e:
        log_info = {
            "status": "完成",
            "endDate": getLocalDate(),
            "count": 0,
            "result": "失败",
            "detail": str(e)
        }
        if not generated_log_id:
            updateLogToDB(cur=cursor, log_id=generated_log_id, properties=log_info)
            conn.commit()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if session:
            session.close()
