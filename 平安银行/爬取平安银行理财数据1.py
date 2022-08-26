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


def crawl(driver: WebDriver, cursor,
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
        rows = parse_table(table, table_columns,
                           callbacks,
                           extra_attrs)
        for row in rows:
            insertLogToDB(cursor, row, TARGET_TABLE)

    select_type_func()  # 选中目标类型
    click_search_func()  # 点击查询
    parse_html()  # 解析当前html
    try:
        page = get_total_page(driver)  # 获取页数
        for i in range(1, page):  # 不断循环点击下一页解析
            click_next_page_func()
            parse_html()
    except Exception as e:
        print(e)


def select_target_type(driver, xpath1, xpath2):
    driver.find_element(By.XPATH, xpath1).click()
    document_initialised(driver)
    driver.find_element(By.XPATH, xpath2).click()
    document_initialised(driver)


class SpiderFlowImpl(SpiderFlow):
    def callback(self, conn, cursor, session: Session, log_id: int, **kwargs):
        driver = None
        try:
            driver = get_driver()
            driver.maximize_window()
            driver.get(REQUEST_URL)

            # 处理-银行理财-数据
            def select_type_func():
                select_target_type(driver, '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[1]/div/i',
                                   '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[2]/ul/li[1]')

            def click_search_func():
                driver.find_element(By.XPATH, '//*[@id="su"]').click()
                document_initialised(driver)

            def click_next_page_func():
                driver.find_element(By.XPATH, '//*[@id="root"]/section/div[3]/div/div/div[5]/div/div[1]/a[3]').click()
                document_initialised(driver)

            table_columns = ['cpbm', 'cpmc', 'fxr', 'fxdj', 'qgje', 'xsqy', 'cpzt', 'cplx', 'djbh', 'pass']
            callbacks = {'qgje': lambda x: str(x) + '元'}
            extra_attrs = {'logId': log_id, 'cpfl': '银行理财', 'cply': '平安银行', 'cpgs': '本行产品'}

            crawl(driver, cursor, select_type_func, click_search_func, click_next_page_func, table_columns,
                  callbacks, extra_attrs)

            # 处理-银行公司理财-数据
            extra_attrs = {'logId': log_id, 'cpfl': '银行公司理财', 'cply': '平安银行', 'cpgs': '本行产品'}

            def select_type_func():
                select_target_type(driver, '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[1]/div/i',
                                   '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[2]/ul/li[2]')

            crawl(driver, cursor, select_type_func, click_search_func, click_next_page_func, table_columns,
                  callbacks, extra_attrs)

            # 处理-对公结构性存款-数据
            extra_attrs = {'logId': log_id, 'cpfl': '对公结构性存款', 'cply': '平安银行', 'cpgs': '本行产品'}

            def select_type_func():
                select_target_type(driver, '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[1]/div/i',
                                   '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[2]/ul/li[10]')

            crawl(driver, cursor, select_type_func, click_search_func, click_next_page_func, table_columns,
                  callbacks, extra_attrs)

            # 处理-个人结构性存款-数据
            extra_attrs = {'logId': log_id, 'cpfl': '对公结构性存款', 'cply': '平安银行', 'cpgs': '本行产品'}

            def select_type_func():
                select_target_type(driver, '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[1]/div/i',
                                   '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[2]/ul/li[3]')

            crawl(driver, cursor, select_type_func, click_search_func, click_next_page_func, table_columns,
                  callbacks, extra_attrs)
            #########################################################################################################
            # 处理-公募基金-数据
            extra_attrs = {'logId': log_id, 'cpfl': '公募基金', 'cply': '平安银行', 'cpgs': '代销产品'}
            table_columns = ['cpbm', 'cpmc', 'tadm', 'fxr', 'pass', 'cpzt', 'fxdj', 'xsqy', 'pass']

            def select_type_func():
                select_target_type(driver, '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[1]/div/i',
                                   '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[2]/ul/li[4]')

            crawl(driver, cursor, select_type_func, click_search_func, click_next_page_func, table_columns,
                  callbacks, extra_attrs)

            # 处理-理财子-数据
            extra_attrs = {'logId': log_id, 'cply': '平安银行', 'cpgs': '代销产品'}
            table_columns = ['cpbm', 'cpmc', 'tadm', 'fxr', 'cpfl', 'cpzt', 'fxdj', 'xsqy', 'pass', 'pass']

            def select_type_func():
                select_target_type(driver, '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[1]/div/i',
                                   '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[2]/ul/li[5]')

            crawl(driver, cursor, select_type_func, click_search_func, click_next_page_func, table_columns,
                  callbacks, extra_attrs)

            # 处理-公司代销理财-数据
            extra_attrs = {'logId': log_id, 'cply': '平安银行', 'cpgs': '代销产品'}
            table_columns = ['cpbm', 'cpmc', 'tadm', 'fxr', 'cpfl', 'cpzt', 'fxdj', 'xsqy', 'pass', 'pass']

            def select_type_func():
                select_target_type(driver, '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[1]/div/i',
                                   '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[2]/ul/li[11]')

            crawl(driver, cursor, select_type_func, click_search_func, click_next_page_func, table_columns,
                  callbacks, extra_attrs)

            # 处理-养老保障-数据
            extra_attrs = {'logId': log_id, 'cply': '平安银行', 'cpfl': '养老保障', 'cpgs': '代销产品'}
            table_columns = ['cpbm', 'cpmc', 'tadm', 'fxr', 'pass', 'cpzt', 'fxdj', 'xsqy', 'pass']

            def select_type_func():
                select_target_type(driver, '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[1]/div/i',
                                   '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[2]/ul/li[6]')

            crawl(driver, cursor, select_type_func, click_search_func, click_next_page_func, table_columns,
                  callbacks, extra_attrs)

            # 处理-保险-数据
            extra_attrs = {'logId': log_id, 'cply': '平安银行', 'cpfl': '保险', 'cpgs': '代销产品'}
            table_columns = ['cpbm', 'cpmc', 'fxr', 'cplx', 'cpzt', 'fxdj', 'pass', 'pass', 'pass']

            def select_type_func():
                select_target_type(driver, '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[1]/div/i',
                                   '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[2]/ul/li[7]')

            crawl(driver, cursor, select_type_func, click_search_func, click_next_page_func, table_columns,
                  callbacks, extra_attrs)

            # 处理-信托/资管计划及其他-数据
            extra_attrs = {'logId': log_id, 'cply': '平安银行', 'cpfl': '信托/资管计划及其他', 'cpgs': '代销产品'}
            table_columns = ['cpbm', 'cpmc', 'tadm', 'fxr', 'cplx', 'cpzt', 'fxdj', 'xsqy', 'pass', 'pass']

            def select_type_func():
                select_target_type(driver, '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[1]/div/i',
                                   '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[2]/ul/li[8]')

            crawl(driver, cursor, select_type_func, click_search_func, click_next_page_func, table_columns,
                  callbacks, extra_attrs)
            # 处理-保险金信托-数据
            extra_attrs = {'logId': log_id, 'cply': '平安银行', 'cpfl': '保险金信托', 'cpgs': '代销产品'}
            table_columns = ['cpmc', 'cplx', 'fxr', 'fxdj', 'pass', 'pass', 'pass']

            def select_type_func():
                select_target_type(driver, '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[1]/div/i',
                                   '//*[@id="root"]/section/div[3]/div/div/div[2]/div/div/div[2]/ul/li[9]')

            crawl(driver, cursor, select_type_func, click_search_func, click_next_page_func, table_columns,
                  callbacks, extra_attrs)

        except Exception as e:
            print(e)
            raise e
        finally:
            close(driver)


if __name__ == '__main__':
    process_flow(LOG_NAME, TARGET_TABLE, SpiderFlowImpl())
