import json
import math
import time
from enum import Enum
from bs4 import BeautifulSoup
from requests import Session
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.wait import WebDriverWait

from utils.common_utils import transform_rows
from utils.html_utils import parse_table
from utils.mark_log import insertLogToDB
from utils.selenium_utils import get_driver, close
from utils.spider_flow import SpiderFlow, process_flow
from selenium.webdriver.support import expected_conditions

REQUEST_URL = 'https://ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true'
TARGET_TABLE = 'ip_bank_payh_personal'
LOG_NAME = '平安银行'


class TypeEnum(Enum):
    get_total_page_enum = '获取总页数'
    get_target_page_data_enum = '获取指定页码数据'


def get_data(driver: WebDriver,
             script_func: str,
             channel_code: str,
             page_num: int,
             typeEnum: TypeEnum,
             page_size: int = None
             ):
    """
        获取请求的总页数/或者获取页面的数据列表
    :param driver:
    :param script_func:
    :param channel_code:
    :param page_size:
    :param page_num:
    :param typeEnum:
    :return:
    """
    if typeEnum == TypeEnum.get_total_page_enum:
        resp = driver.execute_script(f"return {script_func}(1,{page_size},'{channel_code}')")
        loads = json.loads(resp)
        total_page = math.ceil(int(loads['data']['totalSize']) / page_size)
        return total_page
    if typeEnum == TypeEnum.get_target_page_data_enum:
        resp = driver.execute_script(f"return {script_func}({page_num},{page_size},'{channel_code}')")
        loads = json.loads(resp)
        data_list = loads['data']['superviseProductInfoList']
        return data_list


def doCrawl(driver: WebDriver, method: str, page_size: int, channel_code: str, cursor):
    total_page = get_data(driver, method, channel_code, 1, TypeEnum.get_total_page_enum, page_size)
    for page in range(1, total_page + 1, 1):
        rows = get_data(driver, method, channel_code, page, TypeEnum.get_target_page_data_enum, page_size)
        transformed_rows = transform_rows(origin_rows=rows,
                                          # 替换原dict的key值
                                          key_mappings={'minAmount': 'qgje',
                                                        'rateType': '',
                                                        'riskLevel': 'fxdj',
                                                        'prdName': 'cpmc',
                                                        'prdType': '',
                                                        'saleStatus': 'cpzt',
                                                        'prdDetailUrl': 'cpxq_url',
                                                        'prdCode': 'cpbm',
                                                        'templateId': '',
                                                        'productNo': 'cpbh'
                                                        },
                                          # key替换之后处理新dict的value回调函数
                                          callbacks={},
                                          # 原dict中忽略的属性
                                          ignore_attrs=[

                                          ])
        for row in transformed_rows:
            insertLogToDB(cursor, row, TARGET_TABLE)


class SpiderFlowImpl(SpiderFlow):
    def callback(self, conn, cursor, session: Session, log_id: int, **kwargs):
        driver = None
        try:
            driver = get_driver()
            driver.maximize_window()
            driver.get(REQUEST_URL)
            # 爬取-银行理财-数据
            with open('exec.js', 'r', encoding='utf-8') as f:
                js = f.read()
            driver.execute_script(js)
            # 理财产品-总页数
            method = 'doPostLCCPURL'
            channel_code = 'C0002'
            page_size = 10
            doCrawl(driver, method, pa)


        except Exception as e:
            print(e)
            raise e
        finally:
            close(driver)


if __name__ == '__main__':
    process_flow(LOG_NAME, TARGET_TABLE, SpiderFlowImpl())
