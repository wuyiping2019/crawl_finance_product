import json
from dataclasses import dataclass
from typing import Callable, Any

from bs4 import BeautifulSoup
from scrapy import Spider, Request
from scrapy.http import Response
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common import keys
from selenium.webdriver.common.by import By

from scrapy_modules.items import PayhItem, ZGLCWItem
from scrapy_modules.spider_config import SpiderEnum
from utils.db_utils import get_conn, close
from utils.html_utils import parse_table
from utils.mark_log import mark_start_log, getLocalDate, get_generated_log_id, mark_success_log, mark_failure_log
from utils.selenium_utils import get_driver
from utils.zglcw import parse_zglcw_table, ZGLCW_REQUEST_URL


@dataclass
class PayhSpiderMetaSettings:
    spider_type: SpiderEnum
    page_num: int
    total_page: int
    current_page: int


@dataclass(frozen=True)
class PayhSpiderCallbacks:
    # 获取当前页面的总页数
    get_total_page: Callable[[WebDriver], int] = lambda driver: int(
        driver.find_element(By.XPATH, '//*[@id="root"]/section/div[3]/div/div/div[5]/div/div[1]/div[2]/span').text)
    # 点击获取下一页数据
    click_next_page: Callable[[Any], None] = lambda driver: \
        driver.find_element(By.XPATH,
                            '//*[@id="root"]/section/div[3]/div/div/div[3]/div/dl[1]/dd/ul/li[1]') \
            .click()
    # 清空页码数
    clear_page_num: Callable[[WebDriver], Any] = lambda driver: \
        driver.find_element(By.XPATH,
                            '//*[@id="root"]/section/div[3]/div/div/div[5]/div/div[1]/div[2]/input') \
            .clear()
    # 输入下一页
    input_next_page_num: Callable[[WebDriver, int], None] = lambda driver, page_num: \
        driver.find_element(By.XPATH,
                            '//*[@id="root"]/section/div[3]/div/div/div[5]/div/div[1]/div[2]/input') \
            .send_keys(page_num)
    # 输入enter
    update_page_data: Callable[[WebDriver], Any] = lambda driver: \
        driver.find_element(By.XPATH,
                            '//*[@id="root"]/section/div[3]/div/div/div[5]/div/div[1]/div[2]/input') \
            .send_keys(keys.Keys.ENTER)


class PayhSpider(Spider):
    name = '平安银行'

    def init(self):
        conn = get_conn()
        cursor = conn.cursor()
        driver = get_driver()
        self.driver = driver
        self.conn = conn
        self.cursor = cursor

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.successItemCount = 0
        self.failureItemCount = 0
        self.driver = None
        self.conn = None
        self.cursor = None
        self.init()

        self.start_urls = ['https://ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true']
        self.target_table = 'ip_bank_payh_personal'
        self.log_name = '平安银行'
        self.extra_static_attrs = {
            'logId': None,
            'cply': '平安银行'
        }
        self.meta = PayhSpiderMetaSettings(spider_type=SpiderEnum.payh, page_num=1, total_page=-1, current_page=1)
        self.callbacks = PayhSpiderCallbacks()
        # 开始爬虫的日志
        self.logger.info(f'开始爬取{self.log_name}-{self.start_urls[0]}网页的数据')
        self.logger.info(f'记录开始爬虫任务的日志:{getLocalDate()}')
        mark_start_log(name=self.log_name, startDate=getLocalDate(), cursor=self.cursor)
        # 初始化log_id
        self.extra_static_attrs['logId'] = get_generated_log_id(name=self.log_name, cursor=self.cursor)
        self.log_id = self.extra_static_attrs['logId']
        self.logger.info(f"本次任务的logId:{self.extra_static_attrs['logId']}")

    def start_requests(self):
        for url in self.start_urls:
            # 在Request中携带meta信息
            # 在middlerware中通过判断mata信息判断该Request是否由该middleware处理
            yield Request(url, dont_filter=True, meta={'to': SpiderEnum.payh})

    def parse(self, response: Response, **kwargs):
        """
        默认解析Response的方法
        :param response:
        :param kwargs:
        :return: 返回PayhItem对象
        """
        body: str = response.body.decode('utf-8')
        soup = BeautifulSoup(body, 'lxml')
        table = soup.select('table')[0]
        rows = parse_table(table=table,
                           col_names=['cpbm', 'cpmc', 'pass', 'pass', 'qgje', 'xswd', 'cpzt', 'cplx', 'djbm', 'pass'],
                           callbacks={'qgje': lambda x: x + '元'},
                           extra_attrs=self.extra_static_attrs)

        for row in rows:
            # 返回Item
            yield PayhItem(**row)

            # 返回请求中国理财网的请求（在middleware中处理具体请求逻辑）
            print("============返回一个去中国理财网的请求================")
            yield Request(ZGLCW_REQUEST_URL,
                          dont_filter=True,
                          meta={
                              'to': SpiderEnum.zglcw,
                              'djbm': row['djbm'],
                              'logId': self.log_id
                          },
                          callback=lambda resp: ZGLCWItem(**(json.loads(resp.body.decode('utf-8')))))

        # current_page在PayhDownloaderMiddleware完成了更新操作
        # 在PayhDownloaderMiddleware中表示要获取的页码数据,更新之后表示下一页要获取的页码数据
        # parse回调函数 在更新current_page之后调用 表示获取的下一页的数据
        # 当self.meta.current_page <= self.meta.total_page时,需要继续构造请求下一页的Request

        # 返回一个新的Request（获取下一页的数据）
        if self.meta.current_page <= self.meta.total_page:
            # 更新meta
            print("============返回一个去下一页的请求================")
            yield Request(self.start_urls[0], dont_filter=True, meta={'to': SpiderEnum.payh})

    # spider关闭时的回调函数
    def closed(self, reason):
        self.logger.info("记录日志：更新spider_log表")
        if self.failureItemCount != 0:
            mark_failure_log(e=f"成功{self.successItemCount};失败{self.failureItemCount};",
                             endDate=getLocalDate(),
                             generated_log_id=self.log_id,
                             cursor=self.cursor)
        else:
            mark_success_log(str(self.successItemCount), getLocalDate(), self.log_id, self.cursor)
        print(f'==================关闭{self.name}的连接和浏览器驱动===================')
        close([self.cursor, self.conn, self.driver])
        if self.driver:
            self.driver.quit()
        print(reason)
