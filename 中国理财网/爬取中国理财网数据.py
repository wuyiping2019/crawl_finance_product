import datetime
import json
import requests
from pymysql.cursors import Cursor
from crawl_utils.global_config import LOG_TABLE, init_oracle, DB_ENV
from crawl_utils.db_utils import createInsertSql, get_conn_oracle, close
from crawl_utils.mark_log import mark_start_log, mark_success_log, mark_failure_log, get_generated_log_id, get_write_count

NAME = '中国理财网站'
ZGLCW_BANK_TABLE = 'ip_zglcw_bank'
REQUEST_URL = r'https://www.chinawealth.com.cn/dmmsQuery.go'
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
        mark_start_log(NAME, getLocalDate(), cursor)
        # 查询的这条日志信息的自增主键id
        generated_log_id = get_generated_log_id(LOG_TABLE, NAME, cursor)
        # 获取银行信息 写入zglcw_bank表
        session = requests.session()
        data = {
            'code': 0
        }
        resp = session.post(REQUEST_URL, data=data, headers=headers).text
        json_obj = json.loads(resp)
        for item in json_obj:
            # 添加
            item['logId'] = generated_log_id
            writeDataToDB(cur=cursor, properties=item)
        # 统一提交事务
        conn.commit()
        # 查询插入的数据条数
        count = get_write_count(ZGLCW_BANK_TABLE, generated_log_id, cursor)
        # 记录成功日志
        mark_success_log(count, getLocalDate(), generated_log_id, cursor)
    except Exception as e:
        print(e)
        # 记录失败日志
        if cursor:
            cursor.connection.rollback()
            mark_failure_log(e, getLocalDate(), generated_log_id, cursor)
    finally:
        # 释放资源
        close([cursor, conn, session])
