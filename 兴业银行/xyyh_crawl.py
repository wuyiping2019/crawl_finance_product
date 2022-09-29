from requests import Session

from crawl import MutiThreadCrawl
from utils.custom_exception import cast_exception, CustomException
from utils.db_utils import update_else_insert_to_db, check_table_exists, create_table, add_fields, close
from utils.global_config import get_table_name
from utils.mark_log import getLocalDate
from utils.spider_flow import SpiderFlow, process_flow
from xyyh_mobile import process_xyyh_prd_list
from xyyh_pc import process_xyyh_pc
from xyyh_config import TARGET_TABLE_PROCESSED, STR_TYPE, NUMBER_TYPE, SEQUENCE_NAME, TRIGGER_NAME, LOG_NAME, MASK_STR


class SpiderFlowImpl(SpiderFlow):

    def callback(self, session: Session, log_id: int, muti_thread_crawl: MutiThreadCrawl, **kwargs):
        pc_rows = process_xyyh_pc(session)
        mobile_rows = []
        driver = None
        try:
            driver, rows = process_xyyh_prd_list()
        except Exception as e:
            raise e
        finally:
            close(driver)
            if driver:
                driver.quit()
        rows = pc_rows + mobile_rows
        # 检查表是否存在
        poll = muti_thread_crawl.poll
        conn = poll.connection()
        cursor = conn.cursor()
        flag = check_table_exists(TARGET_TABLE_PROCESSED, cursor)
        if not flag:
            create_table(rows[0], STR_TYPE, NUMBER_TYPE, TARGET_TABLE_PROCESSED, cursor, SEQUENCE_NAME,
                         TRIGGER_NAME)
        for row in rows:
            row['logId'] = log_id
            row['createTime'] = getLocalDate()
            try:
                update_else_insert_to_db(cursor, TARGET_TABLE_PROCESSED, row, {'logId': log_id, 'cpbm': row['cpbm']})
            except Exception as e:
                exception = cast_exception(e)
                if isinstance(exception, CustomException) and exception.code == 1:
                    add_fields(cursor, TARGET_TABLE_PROCESSED, row.keys(), STR_TYPE)
                    update_else_insert_to_db(cursor, TARGET_TABLE_PROCESSED, row,
                                             {'logId': log_id, 'cpbm': row['cpbm']})
                else:
                    raise exception


def do_crawl(self: MutiThreadCrawl):
    process_flow(
        log_name=LOG_NAME,
        target_table=get_table_name(mask=MASK_STR),
        callback=SpiderFlowImpl(),
        muti_thread_crawl=self
    )
