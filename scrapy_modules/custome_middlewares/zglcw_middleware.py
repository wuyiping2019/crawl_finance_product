import json
import time
from scrapy import Request
from scrapy.http import Response
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from scrapy_modules.spider_config import SpiderEnum
from utils.zglcw import parse_zglcw_table


# 处理来自url指向的是中国理财网的请求
class ZGLCWDownloaderMiddleware:
    search_url = 'https://www.chinawealth.com.cn/zzlc/jsp/lccp.jsp'

    def process_request(self, request: Request, spider):
        # 在请求的meta信息中添加to字段,来表示这个请求的去向
        # 此处表示该请求需要去中国理财网
        if request.meta.get('to') == SpiderEnum.zglcw:
            driver: WebDriver = spider.driver
            if spider.meta.zglcw:
                pass

            html = driver.execute_script('return document.documentElement.outerHTML')
            print(f"============================{html}===========================")
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
