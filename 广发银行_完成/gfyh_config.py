import json

from crawl_utils.mappings import FIELD_MAPPINGS

LOG_NAME = '广发银行'
MASK = 'gfyh'
SLEEP_SECOND = 3
STATE = 'DEV'


def mobile_yjbjjz(row: dict, key: str) -> tuple:
    yjbjjz = ''
    if {'yieldName', 'yieldName2', 'yieldVal2'}.issubset(row.keys()):
        if row['yieldName'] != '单位净值':
            yjbjjz = json.dumps({
                'title': row['yieldName'],
                'value': row['yieldVal2']
            }).encode().decode('unicode_escape')
        else:
            yjbjjz = json.dumps({
                'title': '预期收益率',
                'value': row['yieldVal2']
            }).encode().decode('unicode_escape')
    return (key, yjbjjz)


# 针对row的value进行转换
MOBILE_FIELD_VALUE_MAPPING = {
    'issPrice': lambda row, key: (key, str(row[key]) + '元'),
    'prdAttr': lambda row, key: (key, '非保本浮动收益类' if str(row[key]) == "1" else '保本浮动收益类'),
    'yjbjjz': mobile_yjbjjz
}
MOBILE_FIELD_NAME_2_NEW_FIELD_NAME = {
    'prdName': FIELD_MAPPINGS['产品名称'],
    'prdName2': FIELD_MAPPINGS['产品简称'],
    'prdCode': FIELD_MAPPINGS['产品编码'],
    'tACode': FIELD_MAPPINGS['TA编码'],
    'tAName': FIELD_MAPPINGS['TA名称'],
    'riskLevel': FIELD_MAPPINGS['风险等级'],
    'investManagerName': FIELD_MAPPINGS['管理人'],
    'issPrice': FIELD_MAPPINGS['发行价'],
    'iPOEndDate': FIELD_MAPPINGS['募集结束日期'],
    'iPOStartDate': FIELD_MAPPINGS['募集起始日期'],
    'estabDate': FIELD_MAPPINGS['产品起始日期'],
    'endDate': FIELD_MAPPINGS['产品结束日期'],
    'curType': FIELD_MAPPINGS['币种'],
    'nAV': FIELD_MAPPINGS['净值'],
    'nAVDate': FIELD_MAPPINGS['净值日期'],
    'totLimitStr': FIELD_MAPPINGS['总额度'],
    'yjbjjz': FIELD_MAPPINGS['业绩比较基准']
}
PC_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
    "Proxy-Connection": "keep-alive",
    "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
}
PC_METHOD = 'POST'
PC_REQUEST_ITER = [
    # 代销资管产品
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598208',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },

        },
        'field_name_2_new_field_name': {
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '产品代码': FIELD_MAPPINGS['产品编码'],
            '产品来源': FIELD_MAPPINGS['管理人'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '起点金额（元）': FIELD_MAPPINGS['起购金额'],
            '产品期限': FIELD_MAPPINGS['投资期限']
        },
        'title': '代销资管产品'
    },
    # 保险
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598600',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },

        },
        'field_value_mapping': {},
        'field_name_2_new_field_name': {
            '类型': FIELD_MAPPINGS['投资性质'],
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '产品代码': FIELD_MAPPINGS['产品编码'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '起点金额（元）': FIELD_MAPPINGS['起购金额'],
        },
        'title': '保险'
    },
    # 公募基金
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598502',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },

        },
        'field_name_2_new_field_name': {
            '产品代码': FIELD_MAPPINGS['产品编码'],
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '产品类型': FIELD_MAPPINGS['募集方式'],
            '产品管理人': FIELD_MAPPINGS['管理人'],
            '销售区域': FIELD_MAPPINGS['销售区域'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '产品状态': FIELD_MAPPINGS['产品状态'],
            '产品净值': FIELD_MAPPINGS['净值']
        },
        'title': '公募基金'
    },
    # 理财产品
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598110',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },

        },
        'field_name_2_new_field_name': {
            '类型': FIELD_MAPPINGS['募集方式'],
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '产品代码': FIELD_MAPPINGS['产品编码'],
            '发行机构': FIELD_MAPPINGS['管理人'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '起点金额（元）': FIELD_MAPPINGS['起购金额'],
            '产品期限': FIELD_MAPPINGS['投资期限'],
        },
        'title': '理财产品'
    },
    # 私募专户
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598404',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },

        },
        'field_name_2_new_field_name': {
            '类型': FIELD_MAPPINGS['募集方式'],
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '产品代码': FIELD_MAPPINGS['产品编码'],
            '产品来源': FIELD_MAPPINGS['管理人'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '起点金额（元）': FIELD_MAPPINGS['起购金额'],
            '持有期限（天）': FIELD_MAPPINGS['投资期限']
        },
        'title': '私募专户'
    },
    # 结构性存款
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/24530070',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },

        },
        'field_value_mapping': {
            '起点金额（元）': lambda row, key: (key, str(row.get(key, None)) + '元' if row.get(key, None) and not str(
                row.get(key, None)).endswith('元') else ''),
        },
        'field_name_2_new_field_name': {
            '类型': FIELD_MAPPINGS['投资性质'],
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '产品代码': FIELD_MAPPINGS['产品编码'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '起点金额（元）': FIELD_MAPPINGS['起购金额'],
            '产品期限': FIELD_MAPPINGS['投资期限']
        },
        'title': '结构性存款'
    },
    # 贵金属
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598698',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },
        },
        'field_value_mapping': {
            '起点金额（元）': lambda row, key: (key, str(row.get(key, None)) + '元' if row.get(key, None) and not str(
                row.get(key, None)).endswith('元') else ''),
        },
        'field_name_2_new_field_name': {
            '类型': FIELD_MAPPINGS['投资性质'],
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '起点金额（元）': FIELD_MAPPINGS['起购金额'],
        },
        'title': '贵金属'
    },
    # 信托产品
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598306',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },

        },
        'field_value_mapping': {
            '起点金额（元）': lambda row, key: (key, str(row.get(key, None)) + '元' if row.get(key, None) and not str(
                row.get(key, None)).endswith('元') else ''),
        },
        'field_name_2_new_field_name': {
            '类型': FIELD_MAPPINGS['募集方式'],
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '产品代码': FIELD_MAPPINGS['产品编码'],
            '产品来源': FIELD_MAPPINGS['管理人'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '起点金额（元）': FIELD_MAPPINGS['起购金额'],
            '持有期限（天）': FIELD_MAPPINGS['投资期限']
        },
        'title': '信托产品'
    },

]
