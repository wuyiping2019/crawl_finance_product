from requests import Session

from config_parser import CrawlConfig, crawl_config
from crawl import MutiThreadCrawl
from crawl_utils.global_config import get_table_name
from crawl_utils.logging_utils import get_logger
from crawl_utils.spider_flow import SpiderFlow, process_flow
from hxyh_config import MASK, LOG_NAME
from 华夏银行_完成.hxyh_mobile import HxyhMobileCrawlRequest
from 华夏银行_完成.hxyh_pc import HxyhPCCrawlRequest

logger = get_logger(__name__)


class SpiderFlowImpl(SpiderFlow):
    def callback(self, session: Session, log_id: int, config: CrawlConfig , **kwargs):
        crawl_pc = HxyhPCCrawlRequest()
        crawl_mobile = HxyhMobileCrawlRequest()
        errors = []
        try:
            crawl_mobile.do_crawl()
        except Exception as e:
            errors.append(e)
            logger.error("处理华夏银行移动端数据失败")
            logger.error(f"{e}")
        finally:
            crawl_mobile.close()

        try:
            crawl_pc.do_crawl()
        except Exception as e:
            errors.append(e)
            logger.error("处理华夏银行PC端数据失败")
            logger.error(f"{e}")
        finally:
            crawl_pc.close()
        if errors:
            logger.error("处理华夏失败,异常个数:%s" % len(errors))


def do_crawl(config: CrawlConfig):
    process_flow(
        log_name=LOG_NAME,
        target_table=get_table_name(mask=MASK),
        callback=SpiderFlowImpl(),
        config=config
    )


if __name__ == '__main__':
    do_crawl(config=crawl_config)
