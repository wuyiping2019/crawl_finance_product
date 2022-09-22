import json

from requests import Session

from gfyh_pc import do_crawl as do_crawl_pc
from gfyh_mobile import do_crawl as do_crawl_mobile
from utils.global_config import get_table_name
from utils.spider_flow import SpiderFlow, process_flow


class SpiderFlowImpl(SpiderFlow):
    def callback(self, conn, cursor, session: Session, log_id: int, **kwargs):
        # crawl_pc = do_crawl_pc(conn, session, log_id)
        crawl_mobile = do_crawl_mobile(session, conn, log_id)
        rows = crawl_mobile.processed_rows
        crawl = crawl_mobile
        crawl.processed_rows = rows
        print(json.dumps(crawl.processed_rows).encode().decode('unicode_escape'))
        crawl.do_save()


if __name__ == '__main__':
    process_flow('广发银行', get_table_name('gfyh'), SpiderFlowImpl())
