from requests import Session

from utils.custom_exception import cast_exception, CustomException
from utils.db_utils import update_else_insert_to_db, check_table_exists, create_table, add_fields
from utils.mark_log import getLocalDate
from utils.spider_flow import SpiderFlow, process_flow
from hxyh_mobile import process_hxyh_mobile
from hxyh_pc import process_hxyh_pc
from hxyh_config import TARGET_TABLE_PROCESSED, STR_TYPE, NUMBER_TYPE, SEQUENCE_NAME, TRIGGER_NAME, LOG_NAME


class SpiderFlowImpl(SpiderFlow):
    def callback(self, conn, cursor, session: Session, log_id: int, **kwargs):
        pc_rows = process_hxyh_pc(session)
        mobile_rows = process_hxyh_mobile(session)
        rows = pc_rows if pc_rows else [] + mobile_rows if mobile_rows else []
        # 检查表是否存在
        flag = check_table_exists(TARGET_TABLE_PROCESSED, cursor)
        if not flag:
            create_table(rows[0], STR_TYPE, NUMBER_TYPE, TARGET_TABLE_PROCESSED, conn.cursor(), SEQUENCE_NAME,
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


if __name__ == '__main__':
    process_flow(LOG_NAME, TARGET_TABLE_PROCESSED, SpiderFlowImpl())
