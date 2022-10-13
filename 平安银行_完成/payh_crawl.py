import traceback

from requests import Session

from config_parser import CrawlConfig, crawl_config
from crawl import MutiThreadCrawl
from crawl_utils.crawl_request import raise_crawl_request_exception
from crawl_utils.global_config import get_table_name
from crawl_utils.logging_utils import get_logger
from crawl_utils.spider_flow import SpiderFlow, process_flow
from payh_config import MASK, LOG_NAME
from 平安银行_完成.payh_pc import PayhPCCrawlRequest

logger = get_logger(__name__)


class SpiderFlowImpl(SpiderFlow):

    def callback(self, session: Session, log_id: int, config: CrawlConfig, **kwargs):
        errors = []
        try:
            PayhPCCrawlRequest().init_props(session=session, log_id=log_id, config=config).do_crawl()
        except Exception as e:
            errors.append(e)
        raise_crawl_request_exception(errors)


def do_crawl(config: CrawlConfig):
    process_flow(
        log_name=LOG_NAME,
        target_table=get_table_name(mask=MASK),
        callback=SpiderFlowImpl(),
        config=config
    )


if __name__ == '__main__':
    do_crawl(config=crawl_config)
