import json
from crawl_utils.db_utils import init_oracle, get_conn_oracle, close
from crawl_utils.mark_log import insertLogToDB, mark_start_log, mark_success_log, mark_failure_log, getLocalDate, \
    get_generated_log_id, updateLogToDB, get_write_count
from crawl_utils.global_config import DB_ENV

import requests

URL = r'https://per.spdb.com.cn/was5/web/search'
NAME = '浦发银行'
TARGET_TABLE = 'ip_bank_spdb_personal'
if DB_ENV == 'ORACLE':
    init_oracle()

headers = {
    'X-Requested-With': 'XMLHttpRequest',
    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'
}


def process_spider(sess, cur, log_id):
    types = [
        {
            'type': '固定期限',
            'searchword': "(product_type=3)*finance_limittime = %*%*(finance_state='可购买')"
        },
        {
            'type': '现金管理类',
            'searchword': "(product_type=4)*finance_limittime = %*%*(finance_state='可购买')"
        },
        {
            'type': '净值类',
            'searchword': "(product_type=2)*finance_limittime = %*%*(finance_state='可购买')"
        },
        {
            'type': '私行专属',
            'searchword': "(product_type=0)*finance_limittime = %*%*(finance_state='可购买')"
        },
        {
            'type': '专属产品',
            'searchword': "(product_type=1)*finance_limittime = %*%*(finance_state='可购买')"
        }
    ]
    process_details(sess, cur, log_id, types)


def process_details(sess, cur, log_id, types):
    """
    处理固定期限的理财数据
    :param sess:
    :param cur:
    :param log_id:
    :return:
    """
    for type in types:
        typeName = type['type']
        searchword = type['searchword']
        data = {
            'metadata': 'finance_state | finance_no | finance_allname | finance_anticipate_rate | finance_limittime | finance_lmttime_info | finance_type | docpuburl | finance_ipo_enddate | finance_indi_ipominamnt | finance_indi_applminamnt | finance_risklevel | product_attr | finance_ipoapp_flag | finance_next_openday',
            'channelid': '266906',
            'page': '1',
            'searchword': searchword
        }
        resp = sess.post(URL, data=data, headers=headers).text
        try:
            resp_dict = json.loads(resp)
        except Exception as e:
            print('请求结果无法解析为json', e, ' 忽略该异常')
            continue
        pageTotal = resp_dict['pageTotal']
        total = resp_dict['total']
        # 循环处理每一页的数据
        for page in range(1, pageTotal + 1, 1):
            data['page'] = str(page)
            response = sess.post(URL, data=data, headers=headers)
            status_code = response.status_code
            if status_code == 200:
                # 当正常请求时，但是结果无法转成对象，忽略异常（没有数据）
                rows = []
                try:
                    rows = json.loads(response.text)['rows']
                except Exception as e:
                    print('请求结果无法解析为json', e, ' 忽略该异常')
                for row in rows:
                    row['logId'] = log_id
                    row['type'] = typeName
                    row = {k: str(v).replace('\'', '') for k, v in row.items()}
                    insertLogToDB(cur, row, TARGET_TABLE)
        cur.connection.commit()
        updateLogToDB(cur, log_id, {'status': '完成《固定期限》类别的数据', 'updateTime': getLocalDate()})
        cur.connection.commit()


if __name__ == '__main__':
    conn = None
    cursor = None
    generated_log_id = None
    session = None
    try:
        conn = get_conn_oracle()
        cursor = conn.cursor()
        # 记录开始日志
        mark_start_log(NAME, getLocalDate(), cursor)
        # 获取日志id
        generated_log_id = get_generated_log_id(NAME, cursor)
        # 处理爬起数据的逻辑
        session = requests.session()
        process_spider(session, cursor, generated_log_id)
        # 统计插入的数据条数
        count = get_write_count(TARGET_TABLE, generated_log_id, cursor)
        # 记录成功日志
        mark_success_log(count, getLocalDate(), generated_log_id, cursor)
    except Exception as e:
        print(e)
        # 回滚事务
        if cursor:
            cursor.connection.rollback()
        # 记录失败日志
        if generated_log_id:
            mark_failure_log(e, getLocalDate(), generated_log_id, cursor)
    finally:
        close([cursor, conn, session])
