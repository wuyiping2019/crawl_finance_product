import json
import re

from crawl_utils.crawl_request import RowKVTransformAndFilter
from crawl_utils.mappings import FIELD_MAPPINGS
from 中国光大银行_完成.zggdyh_config import PATTERN_Z, PATTERN_E


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
mobile_filter1 = RowKVTransformAndFilter(
    filters={
        'issPrice': lambda row, key: (key, str(row[key]) + '元'),
        'prdAttr': lambda row, key: (key, '非保本浮动收益类' if str(row[key]) == "1" else '保本浮动收益类'),
        'yjbjjz': mobile_yjbjjz
    }
)
mobile_filter2 = RowKVTransformAndFilter(
    filters={
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
        'totLimitStr': FIELD_MAPPINGS['总额度']
    }
)
mobile_filter3 = RowKVTransformAndFilter(
    filters=[
        FIELD_MAPPINGS['产品名称'],
        FIELD_MAPPINGS['产品简称'],
        FIELD_MAPPINGS['产品编码'],
        FIELD_MAPPINGS['TA编码'],
        FIELD_MAPPINGS['TA名称'],
        FIELD_MAPPINGS['风险等级'],
        FIELD_MAPPINGS['管理人'],
        FIELD_MAPPINGS['发行价'],
        FIELD_MAPPINGS['募集结束日期'],
        FIELD_MAPPINGS['募集起始日期'],
        FIELD_MAPPINGS['产品起始日期'],
        FIELD_MAPPINGS['产品结束日期'],
        FIELD_MAPPINGS['币种'],
        FIELD_MAPPINGS['净值'],
        FIELD_MAPPINGS['净值日期'],
        FIELD_MAPPINGS['总额度'],
        'yjbjjz'
    ]
)

pc_requests_iter = [
    # 代销资管产品
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598208',
            'method': 'post',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },
            'headers': {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                "Proxy-Connection": "keep-alive",
                "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
            },

        },
        'mask': 'gfyh',
        'field_value_mapping': {},
        'field_name_2_new_field_name': {
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '产品代码': FIELD_MAPPINGS['产品编码'],
            '产品来源': FIELD_MAPPINGS['管理人'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '起点金额（元）': FIELD_MAPPINGS['起购金额'],
            '产品期限': FIELD_MAPPINGS['投资期限']
        },
        'check_props': ['logId', 'cpbm'],
        'title': '代销资管产品'
    },
    # 保险
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598600',
            'method': 'post',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },
            'headers': {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                "Proxy-Connection": "keep-alive",
                "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
            },

        },
        'mask': 'gfyh',
        'field_value_mapping': {},
        'field_name_2_new_field_name': {
            '类型': FIELD_MAPPINGS['投资性质'],
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '产品代码': FIELD_MAPPINGS['产品编码'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '起点金额（元）': FIELD_MAPPINGS['起购金额'],
        },
        'check_props': ['logId', 'cpbm'],
        'title': '保险'
    },
    # 公募基金
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598502',
            'method': 'post',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },
            'headers': {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                "Proxy-Connection": "keep-alive",
                "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
            },

        },
        'mask': 'gfyh',
        'field_value_mapping': {},
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
        'check_props': ['logId', 'cpbm'],
        'title': '公募基金'
    },
    # 理财产品
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598110',
            'method': 'post',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },
            'headers': {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                "Proxy-Connection": "keep-alive",
                "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
            },

        },
        'mask': 'gfyh',
        'field_value_mapping': {},
        'field_name_2_new_field_name': {
            '类型': FIELD_MAPPINGS['募集方式'],
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '产品代码': FIELD_MAPPINGS['产品编码'],
            '发行机构': FIELD_MAPPINGS['管理人'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '起点金额（元）': FIELD_MAPPINGS['起购金额'],
            '产品期限': FIELD_MAPPINGS['投资期限'],
        },
        'check_props': ['logId', 'cpbm'],
        'title': '理财产品'
    },
    # 私募专户
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598404',
            'method': 'post',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },
            'headers': {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                "Proxy-Connection": "keep-alive",
                "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
            },

        },
        'mask': 'gfyh',
        'field_value_mapping': {},
        'field_name_2_new_field_name': {
            '类型': FIELD_MAPPINGS['募集方式'],
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '产品代码': FIELD_MAPPINGS['产品编码'],
            '产品来源': FIELD_MAPPINGS['管理人'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '起点金额（元）': FIELD_MAPPINGS['起购金额'],
            '持有期限（天）': FIELD_MAPPINGS['投资期限']
        },
        'check_props': ['logId', 'cpbm'],
        'title': '私募专户'
    },
    # 结构性存款
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/24530070',
            'method': 'post',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },
            'headers': {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                "Proxy-Connection": "keep-alive",
                "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
            },

        },
        'mask': 'gfyh',
        'field_value_mapping': {
            '起点金额（元）': lambda x: str(x) + '元' if not x.endswith('元') else x,

        },
        'field_name_2_new_field_name': {
            '类型': FIELD_MAPPINGS['投资性质'],
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '产品代码': FIELD_MAPPINGS['产品编码'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '起点金额（元）': FIELD_MAPPINGS['起购金额'],
            '产品期限': FIELD_MAPPINGS['投资期限']
        },
        'check_props': ['logId', 'cpbm'],
        'title': '结构性存款'
    },
    # 贵金属
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598698',
            'method': 'post',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },
            'headers': {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                "Proxy-Connection": "keep-alive",
                "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
            },

        },
        'mask': 'gfyh',
        'field_value_mapping': {
            '起点金额（元）': lambda x: str(x) + '元' if not x.endswith('元') else x,

        },
        'field_name_2_new_field_name': {
            '类型': FIELD_MAPPINGS['投资性质'],
            '产品名称': FIELD_MAPPINGS['产品名称'],
            '风险等级': FIELD_MAPPINGS['风险等级'],
            '起点金额（元）': FIELD_MAPPINGS['起购金额'],
        },
        'check_props': ['logId', 'cpbm'],
        'title': '贵金属'
    },
    # 信托产品
    {
        'request': {
            'url': 'http://www.cgbchina.com.cn/Channel/22598306',
            'method': 'post',
            'json': lambda page_no: {
                'proName': '',
                'proCode': '',
                'curPage': f'{page_no}'
            },
            'headers': {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache",
                "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                "Proxy-Connection": "keep-alive",
                "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
            },

        },
        'mask': 'gfyh',
        'field_value_mapping': {
            '起点金额（元）': lambda x: str(x) + '元' if not x.endswith('元') else x,

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
        'check_props': ['logId', 'cpbm'],
        'title': '信托产品'
    },

]
