import json

from requests import Session

from gfyh_mobile import do_crawl as do_crawl_mobile
from utils.global_config import get_table_name
from utils.spider_flow import SpiderFlow, process_flow
from gfyh_config import SLEEP_SECOND, MASK
from gfyh_mobile2 import gfyh_crawl_mobile
from gfyh_pc import do_crawl


class SpiderFlowImpl(SpiderFlow):
    def callback(self, conn, cursor, session: Session, log_id: int, db_poll=None, **kwargs):
        gfyh_crawl_mobile.session = session
        gfyh_crawl_mobile.db_poll = db_poll
        gfyh_crawl_mobile.do_crawl()
        crawl = do_crawl(conn, session)
        rows = crawl.processed_rows
        print(rows)


if __name__ == '__main__':
    process_flow('广发银行', get_table_name('gfyh'), SpiderFlowImpl())
