import json
import math
import time

from utils.mark_log import insertLogToDB
from utils.selenium_utils import get_driver, close
from utils.spider_flow import process_flow, SpiderFlow

LOG_NAME = '中国民生银行'
TARGET_TABLE = 'ip_bank_zgmsyh_personal'
PAGE_COUNT = 40
REQUEST_URL = 'http://www.cmbc.com.cn/xmhpd/grkh/rmcp/rmlc/index.htm'
METHOD = 'post'


def get_pages(driver):
    result = driver.execute_script('return window.getData(1)')
    loads = json.loads(result)
    return math.ceil(loads['totalSize'] / loads['pageSize'])


def get_target_page(page_no, driver):
    """
    获取的是一页数据的json字符串
    :param page_no:
    :param driver:
    :return:
    """
    return driver.execute_script(f'return window.getData({page_no})')


def parse_page_json(page_json):
    loads = json.loads(page_json)
    prdList = loads['prdList']
    incomeInfos = loads['incomeInfos']
    rows = []
    for prdInfo, incomInfo in zip(prdList, incomeInfos):
        row = {
            'cpmc': prdInfo['prdName'],  # 产品名称
            'cpmcjc': prdInfo['prdShortName'],  # 产品名称简称
            'cpbm': prdInfo['prdCode'],  # 产品编码
            'cply': '中国民生银行',  # 产品来源
            'hblx': prdInfo['currTypeName'],  # 货币类型
            'qgje': str(prdInfo['pfirstAmt']) + '元',  # 起购金额
            'fxdj': prdInfo['riskLevelName'],  # 风险等级
            'cpqx': prdInfo['livTimeUnitName'],  # 产品期限
            'ipo_kssj': prdInfo['ipoStartDate'],  # ipo开始时间
            'ipo_jssj': prdInfo['ipoEndDate'],  # ipo结束时间
            'cped': prdInfo['totAmt'],  # 产品总额度
            'yjbjjz': '%.2f' % (prdInfo['benchMarkMin'] * 100) + '%~' + '%.2f' % (prdInfo['benchMarkMax'] * 100) if incomInfo['incomeTitle'] == '业绩比较基准' else '',  # 业绩比较基准
            'zsjz': prdInfo['remark'],  # 指数基准
            'cplx': incomInfo['prdTypeStr'],  # 产品类型
            'syms': prdInfo['benchMarkBasis'],  # 收益描述
            'xygkfr': incomInfo['nextOpenDate'][0],  # 下一个开放日
            'ksrq': prdInfo['startDate'],  # 开始日期
            'jsrq': prdInfo['realEndDate'],  # 结束日期
            'cpkyed': prdInfo['usableAmt']  # 产品可用额度
        }
        rows.append(row)
    return rows


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
            driver = get_driver()
            driver.get(REQUEST_URL)
            with open(file='exec.js', mode='r', encoding='utf-8') as f:
                js = f.read()
            driver.execute_script(js)
            pages = get_pages(driver)
            for page in range(1, pages + 1, 1):
                time.sleep(1)
                page_json = get_target_page(page, driver)
                rows = parse_page_json(page_json)
                for row in rows:
                    row['logId'] = log_id
                    insertLogToDB(cursor, row, TARGET_TABLE)
        except Exception as e:
            # 此处的异常必须抛出 否则外面的try catch无法捕获异常
            # 也就无法知道此次爬虫是失败还是成功 无法正确记录日志
            print(e)
        finally:
            close(driver)


if __name__ == '__main__':
    process_flow(log_name=LOG_NAME, target_table=TARGET_TABLE, callback=SpiderFlowImpl())
