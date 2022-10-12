import json
import re

from crawl_utils.crawl_request import RowKVTransformAndFilter
from crawl_utils.mappings import FIELD_MAPPINGS
from 中国光大银行_完成.zggdyh_config import PATTERN_Z, PATTERN_E



pc_filter1 = RowKVTransformAndFilter({
    'yjbjjz': lambda row, key: (key, json.dumps(
        {
            'title': row['divModesName'].split(":")[1] if ":" in row['divModesName'] else '指数基准',
            'value': row['divModesName'].split(":")[0] if ":" in row['divModesName'] else '',
        }
    ).encode().decode('unicode_escape'))
})
pc_filter2 = RowKVTransformAndFilter({
    'prdName': FIELD_MAPPINGS['产品名称'],
    'prdShortName': FIELD_MAPPINGS['产品简称'],
    'prdCode': FIELD_MAPPINGS['产品编码'],
    'currTypeName': FIELD_MAPPINGS['币种'],
    'pfirstAmt': FIELD_MAPPINGS['起购金额'],
    'riskLevelName': FIELD_MAPPINGS['风险等级'],
    'ipoStartDate': FIELD_MAPPINGS['募集起始日期'],
    'ipoEndDate': FIELD_MAPPINGS['募集结束日期'],
    'totAmt': FIELD_MAPPINGS['产品额度'],
    'usableAmt': FIELD_MAPPINGS['剩余额度'],
    'startDate': FIELD_MAPPINGS['产品起始日期'],
    'endDate': FIELD_MAPPINGS['产品结束日期'],
    'nextOpenDate': FIELD_MAPPINGS['下一个开放日'],
    'prdTypeName': FIELD_MAPPINGS['运作模式'],
    'NAV': FIELD_MAPPINGS['净值'],
    'navDate': FIELD_MAPPINGS['净值日期'],
    'prdManagerName': FIELD_MAPPINGS['管理人']
})
pc_filter3 = RowKVTransformAndFilter([
    FIELD_MAPPINGS['产品名称'],
    FIELD_MAPPINGS['产品简称'],
    FIELD_MAPPINGS['产品编码'],
    FIELD_MAPPINGS['币种'],
    FIELD_MAPPINGS['起购金额'],
    FIELD_MAPPINGS['风险等级'],
    FIELD_MAPPINGS['募集起始日期'],
    FIELD_MAPPINGS['募集结束日期'],
    FIELD_MAPPINGS['产品额度'],
    FIELD_MAPPINGS['剩余额度'],
    FIELD_MAPPINGS['产品起始日期'],
    FIELD_MAPPINGS['产品结束日期'],
    FIELD_MAPPINGS['下一个开放日'],
    FIELD_MAPPINGS['运作模式'],
    FIELD_MAPPINGS['净值'],
    FIELD_MAPPINGS['净值日期'],
    FIELD_MAPPINGS['管理人'],
    'yjbjjz'
])
