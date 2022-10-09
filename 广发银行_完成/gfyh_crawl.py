from requests import Session

from config_parser import CrawlConfig, crawl_config
from crawl import MutiThreadCrawl
from crawl_utils.crawl_request import CrawlRequestException
from crawl_utils.global_config import get_table_name
from crawl_utils.logging_utils import get_logger
from crawl_utils.spider_flow import SpiderFlow, process_flow
from gfyh_config import MASK, LOG_NAME
from 广发银行_完成.gfyh_mobile import GfyhMobileCrawlRequest
from 广发银行_完成.gfyh_pc import GfyhPCCrawlRequest

logger = get_logger(name=__name__)


class SpiderFlowImpl(SpiderFlow):
    def callback(self, log_id: int, **kwargs):
        crawl_mobile = GfyhMobileCrawlRequest()
        crawl_mobile.log_id = log_id
        crawl_pc = GfyhPCCrawlRequest()
        crawl_pc.log_id = log_id
        error = []
        try:
            crawl_mobile.do_crawl()
            logger.info("")
        except Exception as e:
            logger.error("爬取广发银行移动端端数据失败")
            logger.error(f"{e}")
            error.append(e)
        try:
            crawl_pc.do_crawl()
        except Exception as e:
            logger.error("爬取广发银行PC端数据失败")
            logger.error(f"{e}")
            error.append(e)
        if error:
            print(error)
            logger.error(f"爬取中国民生银行捕获到的异常数量:%s" % len(error))
            error_msg = "\n".join([str(e.__traceback__) for e in error])
            raise CrawlRequestException(None, f"爬取中国民生银行异常{error_msg}")


def do_crawl(config: CrawlConfig):
    process_flow(
        log_name=LOG_NAME,
        target_table=get_table_name(mask=MASK),
        callback=SpiderFlowImpl(),
        config=config
    )
