import copy
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

from utils.string_utils import remove_space

REQUEST_URL = 'https://ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true'
TARGET_TABLE = 'ip_bank_payh_personal'
LOG_NAME = '平安银行'


def document_initialised(driver):
    WebDriverWait(driver, timeout=5).until(lambda d: d.find_element(By.TAG_NAME, "table"))
    time.sleep(3)


def get_total_page(driver: WebDriver):
    page = driver.find_element(By.XPATH,
                               '//*[@id="root"]/section/div[3]/div/div/div[5]/div/div[1]/div[2]').find_element(
        By.TAG_NAME, 'span').text
    return int(page)


def crawl(driver: WebDriver, cursor, log_id,
          select_type_func, click_search_func, click_next_page_func,
          table_columns: list,
          callbacks: dict,
          extra_attrs: dict
          ):
    def parse_html():
        html = driver.execute_script('return document.documentElement.outerHTML')
        document_initialised(driver)
        soup = BeautifulSoup(html, 'lxml')
        table = soup.select('table')[0]
        rows = parse_table(table, ['cpbm', 'cpmc', 'fxr', 'fxdj', 'qgje', 'xsqy', 'cpzt', 'cplx', 'djbh', 'pass'],
                           callbacks={'qgje': lambda x: str(x) + '元'},
                           extra_attrs={
                               'logId': log_id,
                               'cpfl': '银行理财',
                               'cply': '平安银行',
                               'cpgs': '本行产品'
                           })
        for row in rows:
            insertLogToDB(cursor, row, TARGET_TABLE)

    # 爬取-银行理财-数据
    driver.find_element(By.XPATH, '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[1]/div/i').click()
    document_initialised(driver)
    driver.find_element(By.XPATH, '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[2]/ul/li[1]').click()
    document_initialised(driver)
    driver.find_element(By.XPATH, '//*[@id="su"]').click()
    document_initialised(driver)
    parse_html()
    try:
        page = get_total_page(driver)
        for i in range(1, page):
            driver.find_element(By.XPATH, '//*[@id="root"]/section/div[3]/div/div/div[5]/div/div[1]/a[3]').click()
            document_initialised(driver)
            parse_html()
    except Exception as e:
        print(e)


# 本行理财-银行公司理财
class SpiderFlowImpl(SpiderFlow):
    def callback(self, conn, cursor, session: Session, log_id: int, **kwargs):
        driver = None
        try:
            driver = get_driver()
            driver.maximize_window()
            driver.get(REQUEST_URL)
            crawl_yhlc(driver, cursor, log_id)

        except Exception as e:
            print(e)
            raise e
        finally:
            close(driver)


if __name__ == '__main__':
    process_flow(LOG_NAME, TARGET_TABLE, SpiderFlowImpl())
