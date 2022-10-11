import traceback

from requests import Session

from config_parser import CrawlConfig, crawl_config
from crawl_utils.crawl_request import CrawlRequestException, CrawlRequestExceptionEnum
from crawl_utils.global_config import get_table_name
from crawl_utils.logging_utils import get_logger
from crawl_utils.spider_flow import SpiderFlow, process_flow
from 浦发银行_完成.pfyh_config import LOG_NAME, MASK
from 浦发银行_完成.pfyh_pc import PfyhPCCrawlRequest

logger = get_logger(__name__)


class SpiderFlowImpl(SpiderFlow):
    def callback(self, session: Session, log_id: int, config: CrawlConfig, **kwargs):
        crawl_pc = PfyhPCCrawlRequest()
        crawl_pc.session = session
        crawl_pc.log_id = log_id
        crawl_pc.crawl_config = config
        errors = []
        try:
            crawl_pc.do_crawl()
            logger.info("完成浦发银行PC端数据的爬取和保存")
        except Exception as e:
            msg = traceback.format_exc()
            logger.error(msg)
            errors.append(msg)
        if errors:
            logger.error("爬取浦发银行数据,捕获到的异常个数:%s" % len(errors))
            raise CrawlRequestException(
                CrawlRequestExceptionEnum.CRAWL_FAILED_EXCEPTION.code,
                CrawlRequestExceptionEnum.CRAWL_FAILED_EXCEPTION.msg + ':\n' + '\n'.join(errors)
            )


def do_crawl(config: CrawlConfig):
    process_flow(
        log_name=LOG_NAME,
        target_table=get_table_name(MASK),
        callback=SpiderFlowImpl(),
        config=config
    )


if __name__ == '__main__':
    do_crawl(config=crawl_config)
