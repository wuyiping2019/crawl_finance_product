import json
import math
import time

from browsermobproxy import Server
from cx_Oracle import _Error
from requests import Session
from selenium import webdriver
from selenium.webdriver.common.by import By

from utils.db_utils import check_table_exists, create_table, process_dict, check_field_exists, add_field
from utils.global_config import DB_ENV, DBType, get_field_type
from utils.mark_log import insertLogToDB
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


def callbacks(cursor, log_id: int, proxy):
    """
    将数据写入PROCESSED_TABLE中
    :param proxy:
    :param cursor:
    :param log_id:
    :return:
    """
    options = webdriver.ChromeOptions()
    mobile_emulation = {"deviceName": "iPhone X"}
    time.sleep(3)
    options.add_experimental_option("mobileEmulation", mobile_emulation)
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--proxy-server={}'.format(proxy.proxy))
    options.add_experimental_option("excludeSwitches", ["ignore-certificate-errors"])
    driver = get_driver(options=options, driver_path='')
    proxy.new_har(ref="product_list_page", options={'captureContent': True, 'captureBinaryContent': True})
    driver.get('https://m1.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/fund-commission/prd-list')
    time.sleep(3)
    current_index = 0
    # 从此处开始
    proxy.new_har(ref="product_detail_page",
                  options={
                      'captureContent': True,
                      'captureBinaryContent': True
                  }
                  )

    product_count = len(driver.find_elements(By.CLASS_NAME, 'name'))
    while current_index + 1 <= product_count:
        rs: list = proxy.har['log']['entries']
        while len(rs) != 0:
            request_info = rs.pop(0)
            save_dict = {
                'pageref': request_info['pageref'],
                'url': request_info['request']['url'],
                'startedDataTime': request_info['startedDateTime'],
                'response': json.dumps(request_info['response'])
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
                sql = f"""
                        insert into {CRAWL_REQUEST_DETAIL_TABLE} (logid,pageref,startedDataTime,response,url) 
                        values (:logid,:pageref,:startedDataTime,:response,:url)
                        """
                cursor.execute(sql, [log_id, request_info['pageref'], request_info['startedDateTime'],
                                     json.dumps(request_info['response']),
                                     request_info['request']['url']
                                     ]
                               )
                cursor.connection.commit()
            if current_index == 11:
                return
        # 获取一个产品数据的详情
        current_element = driver.find_elements(By.CLASS_NAME, 'name')[current_index]
        current_element.click()
        time.sleep(3)
        # 更新current_index
        current_index += 1
        # 返回到上一页
        proxy.new_har(ref="product_list_page", options={'captureContent': True, 'captureBinaryContent': True})
        driver.back()
        time.sleep(3)
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
                load_more_product(driver)
                time.sleep(3)
                # 更新product_count
                product_count = len(driver.find_elements(By.CLASS_NAME, 'name'))
                footer_element = locate_footer(driver)
                if current_index + 1 <= product_count or footer_element.text == '加载完毕':
                    # 当满足外层循环条件或者已经加载到底部了
                    # 跳出当前加载数据的循环 让外部循环接管逻辑
                    break
