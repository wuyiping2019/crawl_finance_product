import json
import time
from scrapy import Request
from scrapy.http import Response
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from scrapy_modules.spider_config import SpiderEnum
from crawl_utils.zglcw import parse_zglcw_table


# 处理来自url指向的是中国理财网的请求
class ZGLCWDownloaderMiddleware:
    search_url = 'https://www.chinawealth.com.cn/zzlc/jsp/lccp.jsp'
    detail_url = 'https://www.chinawealth.com.cn/zzlc/jsp/lccpDetail.jsp'

    def preprocess(self, driver: WebDriver):
        target_handler = None
        count = 0
        for handler in driver.window_handles:
            driver.switch_to.window(handler)
            current_url = driver.current_url
            if current_url == self.search_url:
                count += 1
                # 关闭多余的查询页
                if count > 1:
                    driver.switch_to.window(handler)
                    driver.close()
                if count == 1:
                    target_handler = handler
            # 关闭详情页
            if current_url == self.detail_url:
                driver.switch_to.window(handler)
                driver.close()
        if target_handler is None:
            driver.get(self.search_url)
            time.sleep(3)
            return self.preprocess(driver)
        else:
            return target_handler

    def process_popup(self, driver, request, spider):
        if driver.find_element(By.CLASS_NAME, 'popup'):
            driver.close()
            self.process_request(request, spider)

    def process_request(self, request: Request, spider):
        # 在请求的meta信息中添加to字段,来表示这个请求的去向
        # 此处表示该请求需要去中国理财网
        if request.meta.get('to') == SpiderEnum.zglcw:
            driver: WebDriver = spider.driver
            # 'https://www.chinawealth.com.cn/zzlc/jsp/lccp.jsp'
            search_handler = self.preprocess(driver)
            driver.switch_to.window(search_handler)
            # 进行"输入登记编码"->"点击查询"->"点击查询结果"->"切换到详情页"->"处理详情页数据"
            driver.find_element(By.XPATH, '//*[@id="cpdjbm"]').clear()
            driver.find_element(By.XPATH, '//*[@id="cpdjbm"]').send_keys(request.meta['djbm'])
            time.sleep(3)
            # 检测是否有弹窗
            self.process_popup(driver,request,spider)
            search_button = driver.find_element(By.CLASS_NAME, 'searchBut')
            driver.execute_script("arguments[0].click();", search_button)
            time.sleep(3)
            # 检测是否有弹窗
            self.process_popup(driver, request, spider)
            element = driver.find_element(By.XPATH, '//*[@id="cptable"]/tbody/tr[2]/td[2]/a')
            driver.execute_script("arguments[0].click();", element)
            time.sleep(3)
            # 检测是否有弹窗
            self.process_popup(driver, request, spider)
            html = driver.execute_script('return document.documentElement.outerHTML')
            print(f"============================{html}=========")
            zglcw_row = parse_zglcw_table(html)
            print("============================", zglcw_row, "============================")
            # 在row上添加logId
            zglcw_row['logId'] = request.meta.get('logId')
            row = json.dumps(zglcw_row).encode('utf-8')
            # 关闭当前窗口
            driver.close()
            return Response(request.url, body=row)
        else:
            return None
