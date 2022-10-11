import traceback

from requests import Session

from config_parser import CrawlConfig, crawl_config
from crawl import MutiThreadCrawl
from crawl_utils.crawl_request import CrawlRequestException, CrawlRequestExceptionEnum
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
            logger.info("成功爬取广发银行移动端的数据并保存")
        except Exception as e:
            logger.error("爬取广发银行移动端端数据失败")
            error.append(traceback.format_exc())
        try:
            crawl_pc.do_crawl()
            logger.info("成功爬取广发银行PC端的数据并保存")
        except Exception as e:
            logger.error("爬取广发银行PC端数据失败")
            error.append(traceback.format_exc())
        if error:
            logger.error(f"爬取《广发银行》捕获到的异常数量:%s" % len(error))
            error_msg = "\n".join([e for e in error])
            raise CrawlRequestException(
                CrawlRequestExceptionEnum.CRAWL_FAILED_EXCEPTION.code,
                CrawlRequestExceptionEnum.CRAWL_FAILED_EXCEPTION.msg + ':\n' + error_msg
            )


def do_crawl(config: CrawlConfig):
    process_flow(
        log_name=LOG_NAME,
        target_table=get_table_name(mask=MASK),
        callback=SpiderFlowImpl(),
        config=config
    )
