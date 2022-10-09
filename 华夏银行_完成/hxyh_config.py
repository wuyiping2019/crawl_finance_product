from crawl_utils.mappings import FIELD_MAPPINGS

LOG_NAME = '华夏银行'
MASK = 'hxyh'
SLEEP_SECOND = 1
STATE = 'DEV'

MOBILE_REQUEST_INFO = {
    'request': {
        'url': 'https://m.hxb.com.cn/wechat/wxbank/finance/finance/netValueFinancial',
        'method': 'post',
        'headers': {"Accept-Encoding": "gzip, deflate, br",
                    "Referer": "https://m.hxb.com.cn/wechat/static/index.html?code=041JnAGa100mUD0NPCJa1IAcNr1JnAGX&state=STATE",
                    "Connection": "keep-alive", "Host": "m.hxb.com.cn", "BL": "null",
                    "User-Agent": "Mozilla\/5.0 (iPhone; CPU iPhone OS 15_6_1 like Mac OS X) AppleWebKit\/605.1.15 (KHTML, like Gecko)  Mobile\/15E148 wxwork\/4.0.16 MicroMessenger\/7.0.1 Language\/zh ColorScheme\/Light",
                    "appid": "1606292992775",
                    "Accept-Language": "zh-CN,zh-Hans;q=0.9",
                    "Accept": "application/json, text/plain, */*",
                    "rev": "DGTwR91tAos2LmB/8vZ+J+PINLDBOgiXe7XGl1AzoDN0s8njrrMD53BSA1INZ5o1N/fAAuUBp1y/lGv59ZEGngwF/Wi+HXTUmYLmM09rIbfMRcZDbdwzCbuprGA23sWTRZW5T/OTLeLIl6b14WRuYL8XmHC9qNmhtMzV73xtJG7J3E66hgRg3jz+z3/oSy/jqYhEKKPQfUwoNV5JO34xcO2ic9c2hQdIcGE0TsUTxJWtX6lvBiba095/pzj38kw5XBg2M99iyKoE4EciulK8Pl0UjDq0Bx2r761BJZGOvHTwHeGxfI9v0MzSNE6wlXJC/dAM4tcGbAZc0k3kyCegVQ=="
                    },

    },
    'identifier': 'hxyh',
    'field_value_mapping': {
    },

    'field_name_2_new_field_name': {
        'productLimit': FIELD_MAPPINGS['投资期限'],
        'productCode': FIELD_MAPPINGS['产品编码'],
        'productName': FIELD_MAPPINGS['产品名称'],
    },
    'check_props': ['logId', 'cpbm'],
    'title': '华夏银行-MOBILE'
}


def process_source_name(source_code):
    source_name = ""
    if not source_code and source_code != 0:
        source_name = ""
    else:
        if '-1' in source_code:
            source_name = source_name + "全部、"
        if '0' in source_code:
            source_name = source_name + "柜面、"
        if '1' in source_code and source_code != '-1':
            source_name = source_name + "电话、"
        if '2' in source_code:
            source_name = source_name + "网银、"
        if '3' in source_code:
            source_name = source_name + "手机、"
        if '4' in source_code:
            source_name = source_name + "支付融资、"
        if 'B' in source_code:
            source_name = source_name + "智能柜台、"
        if 'C' in source_code:
            source_name = source_name + "微信银行、"
        if '8' in source_code:
            source_name = source_name + "e社区、"
        return source_name.strip('、')


PC_REQUEST_INFO = {
    'request': {
        'url': 'https://www.hxb.com.cn/precious-metal-mall/LchqController/findPage',
        'method': 'post',
        'json': {
            "licaiName": "",
            "licaiBuyOriginOrderFlag": "",
            "licaiVieldMaxOrderFlag": "",
            "licaiTimeLimitMaxOrderFlag": ""
        },
        'headers': {"Accept": "application/json, text/javascript, */*; q=0.01",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "Content-Length": "105", "Content-Type": "application/json;charset=UTF-8",
                    "Host": "www.hxb.com.cn",
                    "Origin": "https//www.hxb.com.cn", "Pragma": "no-cache",
                    "Referer": "https//www.hxb.com.cn/grjr/lylc/zzfsdlccpxx/index.shtml",
                    "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                    "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"", "Sec-Fetch-Dest": "empty",
                    "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
                    "X-Requested-With": "XMLHttpRequest"
                    },

    },
    'identifier': 'hxyh',
    'field_value_mapping': {
        'licaiChannelSource': process_source_name,
        'personalFirstBuyLimit': lambda x: str(int(float(x) * 10000)) + '元'
    },

    'field_name_2_new_field_name': {
        'licaiCode': FIELD_MAPPINGS['产品编码'],
        'licaiName': FIELD_MAPPINGS['产品名称'],
        'licaiState': FIELD_MAPPINGS['产品状态'],
        'licaiNewNet': FIELD_MAPPINGS['净值'],
        'licaiRiskLevel': FIELD_MAPPINGS['风险等级'],
        'licaiTimeLimit': FIELD_MAPPINGS['投资期限'],
        'buyBeginDate': FIELD_MAPPINGS['产品起始日期'],
        'licaiExpireDay1': FIELD_MAPPINGS['产品结束日期'],
        'licaiChannelSource': FIELD_MAPPINGS['销售渠道'],
        'personalFirstBuyLimit': FIELD_MAPPINGS['起购金额'],
        '业绩比较基准': FIELD_MAPPINGS['业绩比较基准'],
        '产品说明书': FIELD_MAPPINGS['产品说明书']
    },
    'check_props': ['logId', 'cpbm'],
    'title': '华夏银行-PC'
}
