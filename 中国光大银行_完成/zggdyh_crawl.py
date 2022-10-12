import traceback

from requests import Session

from config_parser import crawl_config, CrawlConfig
from crawl_utils.crawl_request import CrawlRequestException, CrawlRequestExceptionEnum
from crawl_utils.custom_exception import cast_exception, CustomException
from crawl_utils.db_utils import check_table_exists, create_table, insert_to_db, update_else_insert_to_db, add_fields
from crawl_utils.logging_utils import get_logger
from crawl_utils.mark_log import getLocalDate
from crawl_utils.spider_flow import SpiderFlow, process_flow
from zggdyh_config import TARGET_TABLE_PROCESSED, STR_TYPE, NUMBER_TYPE, SEQUENCE_NAME, TRIGGER_NAME, LOG_NAME
from 中国光大银行_完成.zggdyh_pc import ZggdyhPCCrawlRequest

logger = get_logger(__name__)


class SpiderFlowImpl(SpiderFlow):

    def callback(self, session: Session, log_id: int, config: CrawlConfig, **kwargs):
        crawl_pc = ZggdyhPCCrawlRequest().init_props(session=session, log_id=log_id, config=config)
        try:
            crawl_pc.do_crawl()
        except Exception as e:
            pc_error_msg = traceback.format_exc()
            raise CrawlRequestException(
                CrawlRequestExceptionEnum.CRAWL_FAILED_EXCEPTION.code,
                CrawlRequestExceptionEnum.CRAWL_FAILED_EXCEPTION.msg + ':' + pc_error_msg
            )


def do_crawl(config: CrawlConfig):
    process_flow(
        LOG_NAME,
        TARGET_TABLE_PROCESSED,
        SpiderFlowImpl(),
        config=config
    )


if __name__ == '__main__':
    process_flow(LOG_NAME,
                 TARGET_TABLE_PROCESSED,
                 SpiderFlowImpl(),
                 config=crawl_config)
