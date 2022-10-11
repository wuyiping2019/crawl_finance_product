import time

from scrapy import Request
from scrapy.http import Response
from scrapy_modules.spider_config import SpiderEnum
from scrapy_modules.spiders.payh_spider import PayhSpider


class PayhDownloaderMiddleware:
    def process_request(self, request: Request, spider: PayhSpider):
        """
        只处理来自payh_spider的Request
        :param request: PayhSpider对象调用start_requests方法生成的Request对象
        :param spider: PayhSpider对象
        :return:
        """
        flag = False
        # 判断该Request是否来自PayhSpider
        if request.meta['to'] == SpiderEnum.payh:
            flag = True
        # flag == True 该downloader middleware处理Request 否则return None
        if flag:
            # 打开request中的指定网址
            spider.driver.get(request.url)
            time.sleep(1)
            # 参数设置
            # 1.设置总页数
            # 1.1 当总页码数<0时,表示总页码数未赋值
            if spider.meta.total_page < 0:
                spider.meta.total_page = spider.callbacks.get_total_page(driver=spider.driver)
            # 2. 切换页数
            spider.callbacks.clear_page_num(spider.driver)
            spider.callbacks.input_next_page_num(spider.driver, spider.meta.current_page)
            spider.callbacks.update_page_data(spider.driver)
            time.sleep(3)
            # 2.处理spider.meta.current_page的数据
            html: str = spider.driver.execute_script("return document.documentElement.outerHTML")
            # 3.更新spider.meta.currentPage
            spider.meta.current_page += 1
            return Response(url=request.url, body=html.encode('utf-8'))
        else:
            # 表示这个Downloader middleware不处理这个Request
            return None
