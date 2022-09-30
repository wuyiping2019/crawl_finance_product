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
from utils.logging_utils import log
from utils.mappings import FIELD_MAPPINGS
from utils.selenium_utils import get_driver
from utils.string_utils import remove_space
from xyyh_config import SLEEP_SECOND


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


class XyyhCrawlRequest(AbstractCrawlRequest):
    def _scroll_down(self, driver: WebDriver, current_prd_index):
        """
        点击更多来展示更多的产品
        :param driver:
        :param current_prd_index:
        :return:
        """
        prd_count = len(driver.find_elements(By.CLASS_NAME, 'prod-title'))
        if current_prd_index + 1 > prd_count:
            # 获取尾部的数据 是否已经到了底部
            text = ''
            try:
                text = driver.find_element(By.XPATH,
                                           '//*[@id="finOnsaleIndex"]/div[2]/app-multi-list/div/div/div/div').text
            except Exception as e:
                pass
            if remove_space(text) == '无更多数据':
                # 当出现了无更多数据时 记录所有的数据
                total_count = getattr(self, 'total_count', None)
                if not total_count:
                    try:
                        text = driver.find_element(By.XPATH,
                                                   '//*[@id="finOnsaleIndex"]/div[2]/app-multi-list/div/div/div/div').text
                        if remove_space(text) == '无更多数据':
                            total_count = len(driver.find_elements(By.CLASS_NAME, 'prod-title'))
                            setattr(self, 'total_count', total_count)
                    except Exception as e:
                        pass
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
                    self._scroll_down(driver, current_prd_index)

    def _click_all(self):
        # 在产品列表页中点击全部按钮
        driver = getattr(self, 'driver')
        tab_title = driver.find_element(By.CLASS_NAME, 'tab-title')
        li_tags = tab_title.find_elements(By.TAG_NAME, 'li')
        for li_tag in li_tags:
            li_text = remove_space(li_tag.text)
            if li_text == '全部':
                li_tag.click()
                time.sleep(SLEEP_SECOND)
                break

    def _enter_all_prd(self, driver=None):
        # 进入产品列表页
        try:
            url = 'https://z.cib.com.cn/public/fin/onsale/index?type=all'
            if not driver:
                options = webdriver.ChromeOptions()
                mobile_emulation = {"deviceName": "iPhone X"}
                options.add_experimental_option("mobileEmulation", mobile_emulation)
                opened_driver = get_driver(options=options)
                time.sleep(SLEEP_SECOND)

            else:
                opened_driver = driver
            opened_driver.get(url)
            time.sleep(SLEEP_SECOND)
            # 保存driver对象
            setattr(self, 'driver', opened_driver)
            # 点击全部按钮
            self._click_all()
        except Exception as e:
            driver = getattr(self, 'driver', None)
            time.sleep(SLEEP_SECOND)
            self._enter_all_prd(driver=driver)

    def _prep_request(self):
        # 初始化处于全部产品列表页
        self._enter_all_prd()
        # 初始化index
        setattr(self, 'index', 0)
        self._prep_request_flag = True

    def _parse_response(self, response: Response) -> List[dict]:
        rows = getattr(response, 'rows')
        return rows

    def _row_processor(self, row: dict) -> dict:
        return row

    def _row_post_processor(self, row: dict):
        return row

    def _if_end(self, response: Response) -> bool:
        # 判断是否终止
        index = getattr(self, 'index', None)
        total_count = getattr(self, 'total_count', None)
        if total_count and index + 1 == total_count:
            return True
        else:
            return False

    def _do_request(self) -> Response:
        log_where = 'xyyh_mobile.XyyhCrawlRequest._do_request'
        # 覆盖父类的_do_request方法

        # 获取driver
        driver = getattr(self, 'driver')
        # 获取当前需要处理的index
        index = getattr(self, 'index')
        # 获取total_count
        total_count = getattr(self, 'total_count', None)

        # 获取当前页面含有的产品个数
        init_prd_count = len(driver.find_elements(By.CLASS_NAME, 'prod-title'))
        # 当需要处理的产品还没有显示时 需要下拉展示更多直到出现目标产品
        if index + 1 > init_prd_count and not total_count:
            self._scroll_down(driver, index)
        # 点击目标产品
        # 创建一个Response对象
        r = Response()
        try:
            driver.find_element(By.XPATH,
                                f'//*[@id="finOnsaleIndex"]/div[2]/app-multi-list/div/div/div/app-fc-fin-prod-list/div[{index + 1}]').click()
            time.sleep(SLEEP_SECOND)
            # 解析产品详情页
            row = process_xyyh_detail_page(driver, driver.current_url)
            # 返回获取的数据
            # 将数据绑定到Response上
            setattr(r, 'rows', [row])
            r.status_code = 200
            return r
        except Exception as e:
            log(self.logger, 'warn', log_where, f'无法点击到第{index + 1}个产品')
            log(self.logger, 'warn', log_where, f"{e}")
            setattr(r, 'rows', [])
            r.status_code = 400
            return r

    def _next_request(self):
        where = 'xyyh_mobile.XyyhCrawlRequest._next_request'
        # 更新index
        setattr(self, 'index', getattr(self, 'index') + 1)
        # 一个从处于产品列表页开始->进入产品详情页->在此处初始化重新进入到产品列表页
        driver = getattr(self, 'driver')
        driver.back()
        time.sleep(SLEEP_SECOND)
        # 确保当前处于产品列表页
        if 'prdCode' not in driver.current_url:
            # 确实处于产品列表页
            # 重新点击全部-测试过程中发现默认处于推荐中
            try:
                self._click_all()
            except Exception as e:
                log(self.logger, 'warn', where, '无法点击全部产品按钮')
                # 重新进入
                self._enter_all_prd(driver=driver)

        else:
            # 没有处于产品列表页
            # 重新进入
            self._enter_all_prd(driver=driver)

    def close(self):
        super().close()
        driver = getattr(self, 'driver', None)
        if driver:
            driver.quit()


xyyh_crawl_mobile = XyyhCrawlRequest()

__all__ = ['xyyh_crawl_mobile']
