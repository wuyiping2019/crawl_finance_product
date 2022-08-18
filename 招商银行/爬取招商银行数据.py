import pymysql
from pymysql.converters import escape_string
from utils.selenium_utils import get_driver, open_chrome
import json
from utils.global_config import DB_ENV, init_oracle
from utils.db_utils import get_conn_oracle, close
from utils.mark_log import mark_start_log, mark_success_log, mark_failure_log, getLocalDate, get_write_count, \
    get_generated_log_id, insertLogToDB

NAME = '招商银行'
TABLE_NAME = 'ip_bank_cmb_personal'
URL = r'http://www.cmbchina.com/cfweb/Personal/'
if DB_ENV == 'ORACLE':
    init_oracle()

if __name__ == '__main__':

    conn = None
    cursor = None
    driver = None
    generated_log_id = None
    # 记录开始日志
    try:
        conn = get_conn_oracle()
        cursor = conn.cursor()
        # 记录开始日志
        mark_start_log(NAME, getLocalDate(), cursor)
        # 获取日志id
        generated_log_id = get_generated_log_id(NAME, cursor)

        driver = get_driver()
        open_chrome(driver, URL)
        with open('crawler.js', encoding='utf-8') as f:
            js_str = f.read()
        driver.execute_script(js_str)
        productListRS = driver.execute_script("return window.getTotalData()")
        products = json.loads(productListRS)
        # 写入数据
        for product in products:
            print(product)
            product['logId'] = generated_log_id
            # product是一个字典，本身含有一个Id属性，对这个属性重命名
            product['innerId'] = product.pop('Id')
            product = {k: escape_string(str(v).replace('\'', '') if v else '') for k, v in product.items()}
            insertLogToDB(cursor, product, TABLE_NAME)
        # 提交事务
        cursor.connection.commit()
        # 获取插入数据条数
        count = get_write_count(TABLE_NAME, generated_log_id, cursor)
        # 写入成功日志
        mark_success_log(count, getLocalDate(), generated_log_id, cursor)

    except Exception as e:
        print(e)
        if cursor:
            cursor.connection.rollback()
        # 写入失败日志
        if generated_log_id:
            mark_failure_log(e, getLocalDate(), generated_log_id, cursor)
    finally:
        close([driver, cursor, conn])
        if driver:
            driver.quit()
