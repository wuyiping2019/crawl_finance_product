from requests import Session
from crawl import MutiThreadCrawl
from utils.global_config import get_table_name
from utils.spider_flow import SpiderFlow, process_flow
from payh_config import MASK, LOG_NAME
from 平安银行.payh_pc import payh_crawl_pc


class SpiderFlowImpl(SpiderFlow):
    def callback(self, session: Session, log_id: int, muti_thread_crawl: MutiThreadCrawl, **kwargs):
        payh_crawl_pc.init_props(session=session,
                                 log_id=log_id,
                                 muti_thread_crawl=muti_thread_crawl,
                                 **kwargs)

        try:

            payh_crawl_pc.do_crawl()
            payh_crawl_pc.do_save()
        except Exception as e:
            raise e
        finally:
            payh_crawl_pc.close()


def do_crawl(self: MutiThreadCrawl):
    process_flow(
        log_name=LOG_NAME,
        target_table=get_table_name(mask=MASK),
        callback=SpiderFlowImpl(),
        muti_thread_crawl=self
    )
