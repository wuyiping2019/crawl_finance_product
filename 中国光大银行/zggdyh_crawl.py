import requests
from requests import Session

from utils.custom_exception import raise_exception, cast_exception, CustomException
from utils.db_utils import check_table_exists, create_table, insert_to_db, update_else_insert_to_db, add_fields
from utils.mark_log import getLocalDate
from utils.spider_flow import SpiderFlow, process_flow
from zggdyh_jzx import process_jzx_type
from zggdyh_yjjz import process_yjjz_type
from zggdyh_yqsy import process_yqsy_type
from zggdyh_config import TARGET_TABLE_PROCESSED, STR_TYPE, NUMBER_TYPE, SEQUENCE_NAME, TRIGGER_NAME, LOG_NAME


def process_row(cursor, row: dict, log_id: int, cpbm: str):
    pass


class SpiderFlowImpl(SpiderFlow):
    def callback(self, conn, cursor, session: Session, log_id: int, **kwargs):
        yqsy_rows = process_yqsy_type(session)
        yjjz_rows = process_yjjz_type(session)
        jzx_rows = process_jzx_type(session)
        rows = yqsy_rows + yjjz_rows + jzx_rows
        # 检查表是否存在
        flag = check_table_exists(TARGET_TABLE_PROCESSED, cursor)
        # 不存在的话 需要先建表
        if not flag:
            create_table(rows[0], STR_TYPE, NUMBER_TYPE, TARGET_TABLE_PROCESSED, cursor, SEQUENCE_NAME, TRIGGER_NAME)
        # 遍历所有的数据 执行更新操作 如果不存在则执行插入操作
        for row in rows:
            row['logId'] = log_id
            row['createTime'] = getLocalDate()
            try:
                # 尝试将数据插入到数据库中 插入过程中可能存在插入的数据字段不存在的问题
                update_else_insert_to_db(cursor, TARGET_TABLE_PROCESSED, row, {'logId': log_id, 'cpbm': row['cpbm']})
            except Exception as e:
                # 转换已经集中定义处理的异常
                error = cast_exception(e)
                # 如果转转之后的异常是自己定义的异常并且异常的code==1表示插入oracle数据中存在插入不存在的字段
                # 此时添加字段之后重新再插入一次
                if isinstance(error, CustomException) and error.code == 1:
                    add_fields(cursor.connection.cursor(), TARGET_TABLE_PROCESSED, list(row.keys()), STR_TYPE)
                    update_else_insert_to_db(cursor, TARGET_TABLE_PROCESSED, row,
                                             {'logId': log_id, 'cpbm': row['cpbm']})
                else:
                    raise e


if __name__ == '__main__':
    process_flow(LOG_NAME, TARGET_TABLE_PROCESSED, SpiderFlowImpl())
