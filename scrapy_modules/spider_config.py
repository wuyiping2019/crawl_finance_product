import dataclasses
from enum import Enum

# 定义记录日志的表
LOG_TABLE = 'spider_log'
LOG_TABLE_DETAIL = 'spider_log_detail'


class SpiderEnum(Enum):
    # 平安银行
    payh = '平安银行'
    # 中国理财网
    zglcw = '中国理财网'


@dataclasses.dataclass
class SpiderLogDetail:
    error_msg: str
