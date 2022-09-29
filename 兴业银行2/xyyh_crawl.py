from requests import Session
from crawl import MutiThreadCrawl
from utils.global_config import get_table_name
from utils.spider_flow import SpiderFlow, process_flow
from xyyh_config import MASK, LOG_NAME
from 兴业银行2.xyyh_mobile import xyyh_crawl_mobile
from 兴业银行2.xyyh_pc import xyyh_crawl_pc


class SpiderFlowImpl(SpiderFlow):
    def callback(self, session: Session, log_id: int, muti_thread_crawl: MutiThreadCrawl, **kwargs):
        xyyh_crawl_pc.init_props(session=session,
                                 log_id=log_id,
                                 muti_thread_crawl=muti_thread_crawl,
                                 **kwargs)
        xyyh_crawl_mobile.init_props(session=session,
                                     log_id=log_id,
                                     muti_thread_crawl=muti_thread_crawl,
                                     **kwargs)
        try:

            xyyh_crawl_mobile.do_crawl()
            xyyh_crawl_mobile.do_save()
        except Exception as e:
            raise e
        finally:
            xyyh_crawl_mobile.close()


def do_crawl(self: MutiThreadCrawl):
    process_flow(
        log_name=LOG_NAME,
        target_table=get_table_name(mask=MASK),
        callback=SpiderFlowImpl(),
        muti_thread_crawl=self
    )
