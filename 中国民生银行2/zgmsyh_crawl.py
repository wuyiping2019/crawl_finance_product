from requests import Session
from crawl import MutiThreadCrawl
from utils.global_config import get_table_name
from utils.spider_flow import SpiderFlow, process_flow
from 中国民生银行2.zgmsyh_config import MASK, LOG_NAME
# from 中国民生银行2.zgmsyh_pc import zgmsyh_crawl_pc
from 中国民生银行2.zgmsyh_mobile import zgmsyh_crawl_mobile


class SpiderFlowImpl(SpiderFlow):
    def callback(self, session: Session, log_id: int, muti_thread_crawl: MutiThreadCrawl, **kwargs):
        # zgmsyh_crawl_pc.init_props(session=session,
        #                            log_id=log_id,
        #                            muti_thread_crawl=muti_thread_crawl,
        #                            **kwargs)
        zgmsyh_crawl_mobile.init_props(session=session,
                                       log_id=log_id,
                                       muti_thread_crawl=muti_thread_crawl,
                                       **kwargs)
        try:
            # zgmsyh_crawl_pc.do_crawl()
            zgmsyh_crawl_mobile.do_crawl()
            # zgmsyh_crawl_mobile.processed_rows = zgmsyh_crawl_pc.processed_rows + zgmsyh_crawl_mobile.processed_rows
            zgmsyh_crawl_mobile.do_save()
        except Exception as e:
            raise e
        finally:
            # zgmsyh_crawl_pc.close()
            zgmsyh_crawl_mobile.close()


def do_crawl(self: MutiThreadCrawl):
    process_flow(
        log_name=LOG_NAME,
        target_table=get_table_name(mask=MASK),
        callback=SpiderFlowImpl(),
        muti_thread_crawl=self
    )
