import re

MARK_STR = 'zggdyh'
LOG_NAME = '中国光大银行'
SEQUENCE_NAME = f'seq_{MARK_STR}'
TRIGGER_NAME = f'trigger_{MARK_STR}'
CRAWL_REQUEST_DETAIL_TABLE = f'crawl_{MARK_STR}'

TARGET_TABLE_PROCESSED = f'ip_bank_{MARK_STR}_processed'
STR_TYPE = 'clob'
NUMBER_TYPE = 'number(11)'
SLEEP_SECOND = 0.5
PATTERN_Z = re.compile(r'[(](.*)[)]', re.S)
PATTERN_E = re.compile(r'[（](.*)[）]', re.S)
