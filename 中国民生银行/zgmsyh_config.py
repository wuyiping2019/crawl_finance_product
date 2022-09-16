import re
from enum import Enum

LOG_NAME = '中国民生银行'
MASK_STR = 'zgmsyh'
SEQUENCE_NAME = f'seq_{MASK_STR}'
TRIGGER_NAME = f'trigger_{MASK_STR}'
TARGET_TABLE_PROCESSED = f'ip_bank_{MASK_STR}_processed'
STR_TYPE = 'clob'
NUMBER_TYPE = 'number(11)'
PATTERN_Z = re.compile(r'[(](.*)[)]', re.S)
PATTERN_E = re.compile(r'[（](.*)[）]', re.S)
# 写入数据之前是否删除表
DROP_TABLE = True
# 休眠时间
SLEEP_SECOND = 3
# 字段映射
FIELD_MAPPINGS = {
    '产品名称': 'cpmc',
    '产品编码': 'cpbm',
    '产品简称': 'cpjc',
    '币种': 'bz',
    '起购金额': 'qgje',
    '风险等级': 'fxdj',
    '募集起始日期': 'mjqsrq',
    '募集结束日期': 'mjjsrq',
    '产品额度': 'cped',
    '剩余额度': 'syed',
    '业绩比较基准': 'yjbjjz',
    '下一个开放日': 'xygkfr',
    '产品起始日期': 'cpqsrq',
    '产品结束日期': 'cpjsrq',
    '运作模式': 'yzms',
    '净值': 'jz',
    '净值日期': 'jzrq',
    '管理人': 'glr',
    '总额度': 'zed',
    '销售渠道': 'xsqd',
    '产品状态': 'cpzt',
    '委托人': 'wtr',
    '历史净值': 'lsjz',
    '赎回规则':'shgz',
    '投资期限':'tzqx'
}


class FieldEnum(Enum):
    产品名称 = 'cpmc'
