from cx_Oracle import LOB

from utils.db_utils import get_conn


def zxyh_process_data(cursor, log_id):
    for row in cursor.execute("select * from crawl_zxyh where logId = :log_id", [log_id]):
        cols = [d[0] for d in cursor.description]
        row_dict = {str(k).upper(): str(v.read()).encode().decode('unicode_escape') if isinstance(v, LOB) else v for k, v in zip(cols, row)}
        if row_dict['URL'] == 'https://m1.cmbc.com.cn/gw/m_app/FinFundProductList.do':
            print(row_dict['RESPONSE'])
            break


if __name__ == '__main__':
    conn = get_conn()
    cursor = conn.cursor()
    zxyh_process_data(cursor, log_id=1022)
