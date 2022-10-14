from requests import Session

from config_parser import CrawlConfig
from crawl_utils.crawl_request import CrawlRequestException
from crawl_utils.global_config import get_table_name
from crawl_utils.logging_utils import get_logger
from crawl_utils.spider_flow import SpiderFlow, process_flow
from 中国民生银行_完成.zgmsyh_config import MASK, LOG_NAME
from 中国民生银行_完成.zgmsyh_mobile import ZgmsyhMobileCrawlRequest
from 中国民生银行_完成.zgmsyh_pc import ZgmsyhPCCrawlRequest

logger = get_logger(name=__name__)


class SpiderFlowImpl(SpiderFlow):
    def callback(self, session: Session, log_id: int, config: CrawlConfig, **kwargs):
        logger.info("开始执行爬取《中国民生银行》的回调函数")
        logger.info("正在初始化ZgmsyhMobileCrawlRequest...")
        crawl_mobile = ZgmsyhMobileCrawlRequest().init_props(session=session, log_id=log_id, config=config)
        logger.info("成功获取ZgmsyhMobileCrawlRequest对象")
        logger.info("正在初始化ZgmsyhPCCrawlRequest对象...")
        crawl_pc = ZgmsyhPCCrawlRequest().init_props(session=session, log_id=log_id, config=config)
        logger.info("成功获取ZgmsyhPCCrawlRequest对象")
        error = []
        try:
            crawl_mobile.do_crawl()
        except Exception as e:
            logger.error("爬取中国民生银行移动端数据失败")
            logger.error(f"{e}")
            error.append(e)
        try:
            crawl_pc.do_crawl()
        except Exception as e:
            logger.error("爬取中国民生银行PC端数据失败")
            logger.error(f"{e}")
            error.append(e)
        if error:
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


if __name__ == '__main__':
    do_crawl(config=CrawlConfig())
