import traceback

from requests import Session

from config_parser import CrawlConfig, crawl_config
from crawl_utils.global_config import get_table_name
from crawl_utils.logging_utils import get_logger
from crawl_utils.spider_flow import SpiderFlow, process_flow
from 招商银行_完成.zsyh_config import LOG_NAME
from 招商银行_完成.zsyh_pc import ZsyhPCCrawlRequest
from 招商银行_完成.爬取招商银行数据 import MASK

logger = get_logger(__name__)


class SpiderFlowImpl(SpiderFlow):
    def callback(self, session: Session, log_id: int, config: CrawlConfig, **kwargs):
        crawl_pc = ZsyhPCCrawlRequest().init_props(session=session, log_id=log_id, config=config)
        crawl_pc.do_crawl()


def do_crawl(config: CrawlConfig):
    process_flow(
        log_name=LOG_NAME,
        target_table=get_table_name(mask=MASK),
        callback=SpiderFlowImpl(),
        config=config
    )


if __name__ == '__main__':
    do_crawl(config=crawl_config)
