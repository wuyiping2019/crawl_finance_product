import json
import math
import time

from utils.mark_log import insertLogToDB
from utils.selenium_utils import get_driver, open_chrome
from utils.spider_flow import process_flow, SpiderFlow

LOG_NAME = '华夏银行'
TARGET_TABLE = 'ip_bank_hxyh_personal'
PAGE_COUNT = 40


class SpiderFlowImpl(SpiderFlow):
    """
    继承SpiderFlow,实现其唯一的抽象方法,该方法定义具体的爬取数据和处理数据的过程
    """

    def callback(self, conn, cursor, session, log_id, **kwargs):
        """
        实现具体处理数据的过程
        :param conn: 数据库连接的Connection对象
        :param cursor: conn.cursor()获取的指针对象
        :param session: requests.session()获取的session对象
        :param log_id: 本次爬取数据过程在日志表中生成的一条反应爬取过程的日志数据的唯一id
        :param kwargs: 额外需要的参数
        :return:
        """
        driver = None
        try:
            # 获取浏览器驱动
            driver = get_driver()
            # 打开华夏银行网址
            open_chrome(driver, 'https://www.hxb.com.cn/index.shtml')
            # 进入华夏银行个人理财网址
            driver.execute_script("window.open('https://www.hxb.com.cn/grjr/lylc/zzfsdlccpxx/index.shtml')")
            with open('exec.js', encoding='utf-8', mode='r') as f:
                js = f.read()
            driver.execute_script(js)
            total_row_count = driver.execute_script('return window.get_total_count()')
            total_page = math.ceil(int(total_row_count) / PAGE_COUNT)
            for page in range(1, total_page + 1, 1):
                time.sleep(1)
                one_page = driver.execute_script('return window.get_target_page(%s,%s)' % (page, PAGE_COUNT))
                prd_list = json.loads(one_page)
                for row in prd_list:
                    row['logId'] = log_id
                    insertLogToDB(cursor, row, TARGET_TABLE)
        except Exception as e:
            # 此处的异常必须抛出 否则外面的try catch无法捕获异常
            # 也就无法知道此次爬虫是失败还是成功 无法正确记录日志
            if driver:
                print('关闭driver')
                driver.close()
            if driver:
                print('退出driver')
                driver.quit()
            raise e


if __name__ == '__main__':
    process_flow(log_name=LOG_NAME, target_table=TARGET_TABLE, callback=SpiderFlowImpl())
