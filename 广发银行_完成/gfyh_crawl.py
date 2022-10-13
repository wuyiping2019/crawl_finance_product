import traceback

from requests import Session

from config_parser import CrawlConfig, crawl_config
from crawl_utils.crawl_request import raise_crawl_request_exception
from crawl_utils.global_config import get_table_name
from crawl_utils.spider_flow import SpiderFlow, process_flow
from gfyh_config import MASK, LOG_NAME
from 广发银行_完成.gfyh_mobile import GfyhMobileCrawlRequest
from 广发银行_完成.gfyh_pc import GfyhPCCrawlRequest


class SpiderFlowImpl(SpiderFlow):
    def callback(self, session: Session, log_id: int, config: CrawlConfig, **kwargs):
        errors = []
        try:
            GfyhMobileCrawlRequest().init_props(session=session, log_id=log_id, config=config)
        except Exception as e:
            errors.append(e)
        try:
            GfyhPCCrawlRequest().init_props(session=session, log_id=log_id, config=config)
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
