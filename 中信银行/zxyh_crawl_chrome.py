import json
import subprocess
import requests
from requests import Session
from utils.db_utils import get_conn_oracle, close
from utils.global_config import DB_ENV, init_oracle
from utils.mark_log import mark_start_log, getLocalDate, mark_success_log, get_generated_log_id, get_write_count, \
    mark_failure_log, insertLogToDB

URL = r'https://etrade.citicbank.com/portalweb/cms/getFinaProdList.htm'
METHOD = 'GET'
NAME = '中信银行'
TARGET_TABLE = 'ip_bank_cncb_personal'
if DB_ENV == 'ORACLE':
    init_oracle()

headers = {
    'X-Requested-With': 'XMLHttpRequest',
    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
    'Host': 'etrade.citicbank.com'
}


def process_detail(cur, log_id: int, sess: Session):
    types = [
        {
            'type': '理财',
            "requestParam": {
                'callback': 'jQuery1113030268752832306167_1660787492286',
                'prdCategory': '1',
                'prdCode': '',
                'prdName': '',
                'pageSize': '10',
                'currentPage': '1',
                '_': '1660787492290'
            }
        },
        {
            'type': '结构性存款',
            'requestParam': {
                'callback': 'jQuery1113030268752832306167_1660787492286',
                'prdCategory': '2',
                'prdCode': '',
                'prdName': '',
                'pageSize': '10',
                'currentPage': '1',
                '_': '1660787492291'

            }
        }
    ]
    for type in types:
        type_name = type['type']
        request_param = type['requestParam']
        resp = sess.request(method=METHOD, url=URL, params=request_param).text
        loads = node_exec(resp)
        pageCount = loads['pageCount']
        loop_pages(pageCount, sess, request_param, log_id, type_name, cur)


def loop_pages(page_count, sess, requestParam, log_id, type_name, cur):
    for page in range(1, page_count + 1, 1):
        resp = sess.request(method=METHOD, url=URL, params=requestParam).text
        loads = node_exec(resp)
        for row in loads['resultList']:
            row['logId'] = log_id
            row['type'] = type_name
            insertLogToDB(cur, row, TARGET_TABLE)


def node_exec(resp):
    with open('temp', encoding='utf-8', mode='w') as f:
        f.write(resp)
    cmd = 'node ./define.js temp'
    pipline = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    read = pipline.stdout.read()
    decode = read.decode('utf-8')
    loads = json.loads(decode)
    return loads


if __name__ == '__main__':
    conn = None
    cursor = None
    session = None
    generated_log_id = None
    try:
        conn = get_conn_oracle()
        cursor = conn.cursor()
        session = requests.session()
        # 记录开始日志
        mark_start_log(NAME, getLocalDate(), cursor)
        # 查询日志id
        generated_log_id = get_generated_log_id(NAME, cursor)
        # 处理爬取数据的细节
        process_detail(cursor, generated_log_id, session)
        # 查询插入的数据条数
        count = get_write_count(TARGET_TABLE, generated_log_id, cursor)
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
        close([cursor, conn, session])
