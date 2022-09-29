import json
import time
from typing import List

from requests import Response
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.chromium import webdriver
from selenium.webdriver.common.by import By

from utils.common_utils import delete_empty_value
from utils.crawl_request import AbstractCrawlRequest
from utils.crawl_request_utils import CrawlRequest
from utils.mappings import FIELD_MAPPINGS
from utils.selenium_utils import get_driver
from utils.string_utils import remove_space
from xyyh_config import SLEEP_SECOND


def locate_target_tag(driver: WebDriver, index=10):
    try:
        prd_element = driver.find_element(By.XPATH,
                                          f'//*[@id="finOnsaleIndex"]/div[2]/app-multi-list/div/div/div/app-fc-fin-prod-list/div[{index}]')
        ActionChains(driver) \
            .move_to_element(prd_element) \
            .perform()
        time.sleep(SLEEP_SECOND)
        text = driver.find_element(By.XPATH, '//*[@id="finOnsaleIndex"]/div[2]/app-multi-list/div/div/div/div').text
        if remove_space(text) == '无更多数据':
            return
        else:
            index = len(driver.find_elements(By.CLASS_NAME, 'ui-fin-prodlist'))
            return locate_target_tag(driver, index)
    except Exception as e:
        print(e)
        index = len(driver.find_elements(By.CLASS_NAME, 'ui-fin-prodlist'))
        return locate_target_tag(driver, index)


def scroll_down(driver: WebDriver, current_prd_index):
    prd_count = len(driver.find_elements(By.CLASS_NAME, 'prod-title'))
    if current_prd_index + 1 > prd_count:
        # 获取尾部的数据 是否已经到了底部
        text = ''
        try:
            text = driver.find_element(By.XPATH, '//*[@id="finOnsaleIndex"]/div[2]/app-multi-list/div/div/div/div').text
        except Exception as e:
            pass
        if remove_space(text) == '无更多数据':
            return
        else:
            # 否则需要向下滚动 展示更多的数
            last_element = driver.find_element(By.XPATH,
                                               f'//*[@id="finOnsaleIndex"]/div[2]/app-multi-list/div/div/div/app-fc-fin-prod-list/div[{current_prd_index + 1}]')
            # 向下滚动
            ActionChains(driver) \
                .move_to_element(last_element) \
                .perform()
            time.sleep(SLEEP_SECOND)
            # 再次检测展示的数据条数
            prd_count = len(driver.find_elements(By.CLASS_NAME, 'prod-title'))
            if prd_count > current_prd_index + 1:
                return
            else:
                scroll_down(driver, current_prd_index)


def click_prd(driver: WebDriver, index):
    element = driver.find_element(By.XPATH,
                                  f'//*[@id="finOnsaleIndex"]/div[2]/app-multi-list/div/div/div/app-fc-fin-prod-list/div[{index + 1}]')
    element.click()
    time.sleep(SLEEP_SECOND)


def enter_all_prd(driver=None):
    url = 'https://z.cib.com.cn/public/fin/onsale/index?type=all'
    if not driver:
        options = webdriver.ChromeOptions()
        mobile_emulation = {"deviceName": "iPhone X"}
        options.add_experimental_option("mobileEmulation", mobile_emulation)
        opened_driver = get_driver(options=options)
        time.sleep(SLEEP_SECOND)
        opened_driver.get(url)
        time.sleep(SLEEP_SECOND)
    else:
        opened_driver = driver
    tab_title = opened_driver.find_element(By.CLASS_NAME, 'tab-title')
    li_tags = tab_title.find_elements(By.TAG_NAME, 'li')
    for li_tag in li_tags:
        li_text = remove_space(li_tag.text)
        if li_text == '全部':
            li_tag.click()
            time.sleep(SLEEP_SECOND)
            break
    return opened_driver


def enter_jz_page(driver: WebDriver, times: int):
    # 确认存在income-btn
    try:
        driver.find_element(By.CLASS_NAME, 'income-btn')
    except Exception as e:
        return False
    # 可以重复进行 times表示尝试次数
    if times <= 0:
        return False
    # 确认当前处于产品详情页
    try:
        primay_title_tag = driver.find_element(By.CLASS_NAME, 'primary-title')
        if '产品详情' not in remove_space(primay_title_tag.text):
            return False
    except Exception as e:
        pass
    # 点击进入详情页
    el = driver.find_element(By.CLASS_NAME, 'income-btn')  # 找到元素
    driver.execute_script("$(arguments[0]).click()", el)
    time.sleep(SLEEP_SECOND)
    try:
        # 确认当前处于详情页
        driver.find_element(By.CLASS_NAME, 'onsaleHisNetValue')
        return True
    except Exception as e:
        # 没有找到目标元素
        enter_jz_page(driver, times - 1)


def process_xyyh_detail_page(driver: WebDriver, detail_url: str):
    """
    处理兴业银行详情页的数据
    :param detail_url:
    :param driver:
    :return:
    """
    cpmc = remove_space(driver.find_element(By.CLASS_NAME, 'name').text)
    cpbm = remove_space(driver.find_element(By.CLASS_NAME, 'code').text.replace('编号:', ''))
    yjbjjz = ''
    try:
        yjbjjz = json.dumps({
            'title': remove_space(driver.find_element(By.CLASS_NAME, 'income-txt').text),
            'value': remove_space(driver.find_element(By.CLASS_NAME, 'cash-txt').text)
        }).encode().decode('unicode_escape')
    except Exception as e:
        pass
    qgje = ''
    tzqx = ''
    fxdj = ''
    try:
        onsale_detail_tag = driver.find_element(By.CLASS_NAME, 'onsale-detail-tab')
        tab_info_tags = onsale_detail_tag.find_elements(By.CLASS_NAME, 'tab-info')
        for tab_info_tag in tab_info_tags:
            tab_info_text = remove_space(tab_info_tag.text)
            if '投资期限' in tab_info_text:
                tzqx = tab_info_text.replace('投资期限', '')
            elif '起购金额' in tab_info_text:
                qgje = tab_info_text.replace('起购金额', '')
            elif '风险等级' in tab_info_text:
                fxdj = tab_info_text.replace('风险等级', '')
    except Exception as e:
        pass

    # 净值
    jz = ''
    jzrq = ''
    lsjz = []
    try:
        flag = enter_jz_page(driver, 3)
        if flag:
            nav_tags = driver.find_elements(By.CLASS_NAME, 'history-value-slide')
            for nav_tag in nav_tags:
                if not jz and not jzrq:
                    jz = remove_space(nav_tag.find_element(By.CLASS_NAME, 'time').text)
                    jzrq = remove_space(nav_tag.find_element(By.CLASS_NAME, 'income').text)
                lsjz.append({
                    'jz': remove_space(nav_tag.find_element(By.CLASS_NAME, 'time').text),
                    'jzrq': remove_space(nav_tag.find_element(By.CLASS_NAME, 'income').text)
                })
    except Exception as e:
        pass
    if lsjz:
        lsjz = json.dumps(lsjz).encode().decode('unicode_escape')
    driver.back()
    time.sleep(SLEEP_SECOND)
    # 确认重新进入了产品详情页
    try:
        driver.find_element(By.CLASS_NAME, 'primary-title')
    except Exception as e:
        driver.get(detail_url)
        time.sleep(SLEEP_SECOND)
    row = {
        FIELD_MAPPINGS['产品名称']: cpmc,
        FIELD_MAPPINGS['产品编码']: cpbm,
        FIELD_MAPPINGS['业绩比较基准']: yjbjjz,
        FIELD_MAPPINGS['净值']: jz,
        FIELD_MAPPINGS['净值日期']: jzrq,
        FIELD_MAPPINGS['历史净值']: lsjz,
        FIELD_MAPPINGS['投资期限']: tzqx,
        FIELD_MAPPINGS['风险等级']: fxdj,
        FIELD_MAPPINGS['起购金额']: qgje
    }
    delete_empty_value(row)
    return row


def process_xyyh_prd_list():
    rows = []
    driver = enter_all_prd()
    time.sleep(SLEEP_SECOND)
    index = 0
    total_count = None
    while True:
        init_prd_count = len(driver.find_elements(By.CLASS_NAME, 'prod-title'))
        # 如果将要处理的index没有出现在页面中并且还没有到达产品列表的底部
        if index + 1 > init_prd_count and not total_count:
            scroll_down(driver, index)
        # 获取产品总条数
        if not total_count:
            try:
                text = driver.find_element(By.XPATH,
                                           '//*[@id="finOnsaleIndex"]/div[2]/app-multi-list/div/div/div/div').text
                if remove_space(text) == '无更多数据':
                    total_count = len(driver.find_elements(By.CLASS_NAME, 'prod-title'))
            except Exception as e:
                pass
        # 如果已经到底了页面底部
        if total_count:
            # 判断当前index
            # 需要处理index + 1的数据
            if index + 1 <= total_count:
                pass
            else:
                # 这个终止条件不生效 （原因不明）
                break
        else:
            pass
        # 进入详情页
        try:
            driver.find_element(By.XPATH,
                                f'//*[@id="finOnsaleIndex"]/div[2]/app-multi-list/div/div/div/app-fc-fin-prod-list/div[{index + 1}]').click()
        except Exception as e:
            break
        # 确认已经进入详情页
        if 'prodCode' not in driver.current_url:
            # 表示如果没有进入详情页的话 重新进入循环
            # 此时index没有更新 处理的还是同一条数据
            continue
        time.sleep(SLEEP_SECOND)
        # 处理详情页数据
        row = process_xyyh_detail_page(driver, driver.current_url)
        rows.append(row)
        print(row)
        # 更新index
        index += 1
        # 返回
        driver.back()
        # 确认已经返回到产品展示页面
        if 'prdCode' in driver.current_url:
            # 表示如果没有返回到产品展示页面
            # 则重新使用url进入到产品展示页面
            # 此时index已经更新 重新进入产品展示页之后 继续处理下一条数据
            enter_all_prd(driver)
        time.sleep(SLEEP_SECOND)
    return driver, rows


class XyyhCrawlRequest(AbstractCrawlRequest):

    def _prep_request(self):
        # 在此处打开driver
        driver = enter_all_prd()
        setattr(self, 'driver', driver)
        time.sleep(SLEEP_SECOND)
        setattr(self, 'index', 0)
        setattr(self, 'total_count', None)
        process_xyyh_detail_page

    def _parse_response(self, response: Response) -> List[dict]:

        # 此处从driver所处的当前页面解析html
        driver = getattr(self, 'driver')

    def _row_processor(self, row: dict) -> dict:
        pass

    def _row_post_processor(self, row: dict):
        pass

    def _if_end(self, response: Response) -> bool:
        pass

    def _do_request(self) -> Response:
        # 覆盖父类的_do_request方法 无须任何处理
        pass

    def _next_request(self):
        # 获取driver
        driver = getattr(self, 'driver')
        # 第几个产品
        index = getattr(self, 'index')
        # 进入产品详情页
        driver.find_element(By.XPATH,
                            f'//*[@id="finOnsaleIndex"]/div[2]/app-multi-list/div/div/div/app-fc-fin-prod-list/div[{index + 1}]').click()
        # 确保已经进入详情页
        if 'prodCode' not in driver.current_url:
            # 表示如果没有进入详情页的话 重新进入循环
            # 此时index没有更新 处理的还是同一条数据
            setattr(self, 'failed_enter_detail_page', True)


if __name__ == '__main__':
    process_xyyh_prd_list()
