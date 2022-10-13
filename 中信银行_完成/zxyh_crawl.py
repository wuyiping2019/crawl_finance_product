import traceback

from requests import Session

from config_parser import CrawlConfig
from crawl_utils.global_config import get_table_name
from crawl_utils.logging_utils import get_logger
from crawl_utils.spider_flow import SpiderFlow, process_flow
from 中信银行_完成.zxyh_config import LOG_NAME, MASK
from 中信银行_完成.zxyh_pc import ZxyhPCCrawlRequest

logger = get_logger(__name__)


class SpiderFlowImpl(SpiderFlow):
    def callback(self, session: Session, log_id: int, config: CrawlConfig, **kwargs):
        crawl_pc = ZxyhPCCrawlRequest()
        crawl_pc.init_props(session=session, log_id=log_id, config=config)
        errors = []
        try:
            crawl_pc.do_crawl()
        except Exception as e:
            logger.error(f"爬取{crawl_pc.name}的数据时发生异常,请排查原因")
            errors.append(traceback.format_exc())


def do_crawl(config: CrawlConfig):
    process_flow(
        log_name=LOG_NAME,
        target_table=get_table_name(MASK),
        callback=SpiderFlowImpl(),
        config=config
    )


if __name__ == '__main__':
    do_crawl(config=CrawlConfig())
