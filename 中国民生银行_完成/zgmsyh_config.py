import json
import re

from crawl_utils.mappings import FIELD_MAPPINGS
from 中国光大银行_完成.zggdyh_config import PATTERN_Z, PATTERN_E

LOG_NAME = '中国民生银行'
MASK = 'zgmsyh'
SLEEP_SECOND = 3
STATE = ''
MOBILE_REQUEST_URL = 'https://ment.cmbc.com.cn/gw/pwx_wx/QryProdListOnMarket.do'
MOBILE_REQUEST_METHOD = 'POST'
MOBILE_REQUEST_HEADERS = {
    "Host": "ment.cmbc.com.cn",
    "Origin": "https//ment.cmbc.com.cn",
    "Referer": "https//ment.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/finance/selling-list",
    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36"
}
MOBILE_REQUEST_JSON = ''
MOBILE_FIELD_VALUE_MAPPING = {
    'totAmt': lambda row, key: (key, str(row.get(key, '')) + '元' if row.get(key, '') != '' else ''),
    'yjbjjz': lambda row, key: (key, json.dumps(
        {
            'title': row['divModesName'].split(':')[1].split('(')[0],
            'value': row['divModesName'].split(':')[0]} \
            if row.get('prdTypeName', '') == '净值类周期型' \
               and len(row['divModesName'].split(':')) >= 2 \
               and len((re.findall(PATTERN_Z, row['divModesName']) \
                        + re.findall(PATTERN_E, row['divModesName']))) >= 1 \
            else '').encode().decode('unicode_escape')),
    'channelsName': lambda row, key: (
        key, json.dumps(row.get('channelsName', '').split(';')).encode().decode(
            'unicode_escape') if row.get('channelsName', '') else ''
    ),
    'warmTipsMap': lambda row, key: (
        key, row.get('', {}).get('tipsTitle', '')
    ),
    'divModesName': lambda row, key: (
        key, json.dumps({
            'title': row.get(key, '').split(':')[1], 'value': row.get(key, '').split(':')[0]
        }).encode().decode('unicode_escape')
    )
}
MOBILE_FIELD_NAME_2_NEW_FIELD_NAME = {
    'NAV': FIELD_MAPPINGS['净值'],
    'endDate': FIELD_MAPPINGS['产品结束日期'],
    'channelsName': FIELD_MAPPINGS['销售渠道'],
    'prdCode': FIELD_MAPPINGS['产品编码'],
    'statusName': FIELD_MAPPINGS['产品状态'],
    'currTypeName': FIELD_MAPPINGS['币种'],
    'prdShortName': FIELD_MAPPINGS['产品简称'],
    'prdTrusteeName': FIELD_MAPPINGS['委托人'],
    'ipoEndDate': FIELD_MAPPINGS['募集结束日期'],
    'ipoStartDate': FIELD_MAPPINGS['募集起始日期'],
    'riskLevelName': FIELD_MAPPINGS['风险等级'],
    'livTimeUnitName': FIELD_MAPPINGS['投资期限'],
    'warmTipsMap': FIELD_MAPPINGS['赎回规则'],
    'prdTypeName': FIELD_MAPPINGS['运作模式'],
    'divModesName': FIELD_MAPPINGS['业绩比较基准'],
    'firstAmt': FIELD_MAPPINGS['起购金额'],
    '产品说明书': FIELD_MAPPINGS['产品说明书'],
    '产品合约': FIELD_MAPPINGS['产品合约'],
    '风险揭示书': FIELD_MAPPINGS['风险揭示书'],
    '产品收益结构图': FIELD_MAPPINGS['产品收益结构图']
}
MOBILE_DETAIL_REQUEST_URL = 'https://ment.cmbc.com.cn/gw/pwx_wx/QueryPrdBuyInfo.do'
MOBILE_DETAIL_REQUEST_JSON = lambda cpbm: {
    "request": {
        "header": {
            "appId": "", "appVersion": "",
            "device": {
                "osType": "BROWSER",
                "osVersion": "",
                "uuid": ""
            }
        },
        "body": {
            "prdCode": cpbm,
            "getVipFlag": "1",
            "GroupFlag": "1",
            "isKJTSS": "0"
        }
    }
}
MOBILE_DETAIL_REQUEST_METHOD = 'POST'

PC_REQUEST_URL = 'http://www.cmbc.com.cn/gw/po_web/QryProdListOnMarket.do'
PC_REQUEST_METHOD = 'POST'
PC_REQUEST_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/json;charset=UTF-8",
    "Host": "www.cmbc.com.cn",
    "Origin": "http//www.cmbc.com.cn",
    "Referer": "http//www.cmbc.com.cn/xmhpd/grkh/rmcp/rmlc/index.htm",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}
PC_REQUEST_JSON = lambda page_no: {
    "currTypeList": [],
    "keyWord": "",
    "currentIndex": 0,
    "fundModeList": [],
    "orderFlag": "1",
    "pageNo": page_no,
    "pageSize": 10,
    "pfirstAmtList": [],
    "prdChara": "4",
    "prdTypeNameList": [],
    "$FF_HEADER$": {
        "appId": "",
        "appVersion": "",
        "device": {
            "osType": "BROWSER",
            "osVersion": "",
            "uuid": ""
        }
    }
}
PC_FIELD_VALUE_MAPPINGS = {
    'divModesName': lambda row, key: (key, json.dumps(
        {
            'title': row[key].split(":")[1] if ":" in row[key] else '指数基准',
            'value': row[key].split(":")[0] if ":" in row[key] else '',
        }
    ).encode().decode('unicode_escape'))
}
PC_FIELD_NAME_2_NEW_FIELD_NAME = {
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
    'prdManagerName': FIELD_MAPPINGS['管理人'],
    'divModesName': FIELD_MAPPINGS['业绩比较基准']
}
