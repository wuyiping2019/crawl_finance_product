import json
import math
import time

from browsermobproxy import Server
from cx_Oracle import _Error
from requests import Session
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

from utils.db_utils import check_table_exists, create_table, process_dict, check_field_exists, add_field, insert_to_db, \
    close
from utils.global_config import DB_ENV, DBType, get_field_type
from utils.mark_log import insertLogToDB, getLocalDate
from utils.selenium_utils import get_driver
from utils.spider_flow import SpiderFlow, process_flow

MARK_STR = 'zxyh'
TARGET_TABLE_PC = f'ip_bank_{MARK_STR}_pc'
TARGET_TABLE_MOBILE = f'ip_bank_{MARK_STR}_mobile'
PROCESSED_TABLE = f'ip_bank_{MARK_STR}'
LOG_NAME = '中信银行'
SEQUENCE_NAME = f'seq_{MARK_STR}'
TRIGGER_NAME = f'trigger_{MARK_STR}'

CRAWL_REQUEST_DETAIL_TABLE = f'crawl_{MARK_STR}'


def load_more_product(driver):
    driver.find_element(By.CLASS_NAME, 'am-list-footer').click()


def locate_footer(driver):
    while True:
        try:
            footer_element = driver.find_element(By.CLASS_NAME, 'am-list-footer')
            break
        except Exception as e:
            print(e)
            time.sleep(3)
    return footer_element


def return_to_previous_page(driver: WebDriver):
    driver.back()
    time.sleep(3)
    # 确认确实已经返回上一页
    try:
        driver.find_element(By.CLASS_NAME, 'buying')
        # 正常定位到class name为buying的元素
        # 表示返回上一页失败 重新进入
        driver.get('https://m1.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/fund-commission/prd-list')
        time.sleep(5)
    except Exception as e:
        return


def callback(conn, cursor, session: Session, log_id: int, **kwargs):
    """
    将数据写入PROCESSED_TABLE中
    :param proxy:
    :param cursor:
    :param log_id:
    :return:
    """
    driver = kwargs['driver']
    driver.get('https://m1.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/fund-commission/prd-list')
    time.sleep(3)
    current_index = 0
    # 从此处开始
    proxy.new_har("product_detail_page",
                  options={
                      'captureContent': True,
                      'captureBinaryContent': True
                  }
                  )
    product_count = len(driver.find_elements(By.CLASS_NAME, 'name'))
    while current_index + 1 <= product_count:
        # 获取一个产品数据的详情
        current_element:WebElement = driver.find_elements(By.CLASS_NAME, 'name')[current_index]
        element: WebElement = driver.find_element()
        current_element.click()
        time.sleep(3)
        # 更新current_index
        current_index += 1
        # 返回到上一页
        return_to_previous_page(driver)
        # 返回上一页之后 重置product_count
        product_count = len(driver.find_elements(By.CLASS_NAME, 'name'))
        # 继续获取下一个产品数据的详情
        # 但是可能存在  current_index + 1 > product_count的情况 导致跳出循环
        # 而跳出循环时 只是由于更多的产品数据没有加载
        if current_index + 1 <= product_count:
            # 该情况可以继续循环
            continue
        else:
            # 循环加载更多的数据 直至满足外面的循环条件或者已经加载到底部了
            while True:
                # 加载更多数据
                footer_element = locate_footer(driver)
                footer_element.click()
                time.sleep(3)
                locate_footer(driver)
                # load_more_product(driver)
                time.sleep(3)
                # 更新product_count
                product_count = len(driver.find_elements(By.CLASS_NAME, 'name'))
                if current_index + 1 <= product_count or footer_element.text == '加载完毕':
                    # 当满足外层循环条件或者已经加载到底部了
                    # 跳出当前加载数据的循环 让外部循环接管逻辑
                    break

    for request_info in proxy.har['log']['entries']:
        save_dict = {
            'logId': log_id,
            'pageref': str(request_info['pageref']),
            'url': str(request_info['request']['url']),
            'startedDataTime': str(request_info['startedDateTime']),
            'response': str(json.dumps(request_info['response'])),
            'createTime': str(getLocalDate())
        }
        flag = check_table_exists(CRAWL_REQUEST_DETAIL_TABLE, cursor)
        if not flag:
            create_table(save_dict,
                         'clob',
                         'number(11)',
                         CRAWL_REQUEST_DETAIL_TABLE,
                         cursor,
                         SEQUENCE_NAME,
                         TRIGGER_NAME)
        flag = True
        if flag:
            insert_to_db(cursor, save_dict, CRAWL_REQUEST_DETAIL_TABLE)
            cursor.connection.commit()


if __name__ == '__main__':
    proxy = None
    driver = None
    proxy_server = None
    try:
        proxy_server = Server(r'E:\PycharmProjects\kb_graph_sync\browsermob-proxy-2.1.4\bin\browsermob-proxy.bat')
        proxy_server.start()
        proxy = proxy_server.create_proxy({'trustAllServers': True})
        options = webdriver.ChromeOptions()
        mobile_emulation = {"deviceName": "iPhone X"}
        options.add_experimental_option("mobileEmulation", mobile_emulation)
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--proxy-server={}'.format(proxy.proxy))
        options.add_experimental_option("excludeSwitches", ["ignore-certificate-errors"])
        driver = get_driver(options=options)
        proxy.new_har(ref="product_list_page", options={'captureContent': True, 'captureBinaryContent': True})
        time.sleep(3)
        process_flow(log_name=LOG_NAME,
                     target_table=CRAWL_REQUEST_DETAIL_TABLE,
                     callback=callback,
                     driver=driver  # 以**kwargs字典传入
                     )

    except Exception as e:
        raise e
    finally:
        close([proxy, driver])
        if driver:
            driver.quit()
        if proxy_server:
            proxy_server.stop()
