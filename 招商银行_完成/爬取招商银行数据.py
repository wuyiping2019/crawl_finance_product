import pymysql
from pymysql.converters import escape_string

from config_parser import CrawlConfig, crawl_config
from crawl_utils.db_utils import close
from crawl_utils.selenium_utils import get_driver, open_chrome
import json
from crawl_utils.global_config import DB_ENV, init_oracle, get_table_name
from crawl_utils.mark_log import mark_start_log, mark_success_log, mark_failure_log, getLocalDate, get_write_count, \
    get_generated_log_id, insertLogToDB

NAME = '招商银行_完成'
MASK = 'zsyh'
TABLE_NAME = get_table_name(MASK)
URL = r'http://www.cmbchina.com/cfweb/Personal/'
if DB_ENV == 'ORACLE':
    init_oracle()


def do_crawl(config: CrawlConfig):
    conn = None
    cursor = None
    driver = None
    generated_log_id = None
    count = 0
    # 记录开始日志
    try:
        conn = config.db_pool.connection()
        cursor = conn.cursor()
        # 记录开始日志
        mark_start_log(NAME, getLocalDate(), config.db_pool)
        # 获取日志id
        generated_log_id = get_generated_log_id(NAME, config.db_pool)
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
            mark_failure_log(e, getLocalDate(), generated_log_id, config.db_pool, count)
    finally:
        close([driver, cursor, conn])
        if driver:
            driver.quit()


if __name__ == '__main__':
    do_crawl(config=crawl_config)
