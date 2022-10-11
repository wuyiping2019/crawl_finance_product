import traceback

from requests import Session

from config_parser import CrawlConfig, crawl_config
from crawl import MutiThreadCrawl
from crawl_utils.crawl_request import CrawlRequestException, CrawlRequestExceptionEnum
from crawl_utils.global_config import get_table_name
from crawl_utils.logging_utils import get_logger
from crawl_utils.spider_flow import SpiderFlow, process_flow
from payh_config import MASK, LOG_NAME
from 平安银行_完成.payh_pc import PayhPCCrawlRequest

logger = get_logger(__name__)


class SpiderFlowImpl(SpiderFlow):

    def callback(self, session: Session, log_id: int, config: CrawlConfig, **kwargs):
        crawl_pc = PayhPCCrawlRequest()
        errors = []
        try:
            crawl_pc.do_crawl()
        except Exception as e:
            errors.append(traceback.format_exc())
            logger.error("处理平安银行PC端数据失败")
            raise e
        finally:
            crawl_pc.close()
        if errors:
            logger.error(f"处理平安银行数据,抛出异常个数:%s" % len(errors))
            error_msg = "\n".join([e for e in errors])
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


if __name__ == '__main__':
    do_crawl(config=crawl_config)
