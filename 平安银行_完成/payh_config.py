from crawl_utils.mappings import FIELD_MAPPINGS
from crawl_utils.string_utils import remove_space

LOG_NAME = '平安银行'
MASK = 'payh'
SLEEP_SECOND = 3
STATE = 'DEV'
FIELD_VALUE_MAPPING = {
    'minAmount': lambda row, key: str(row.get(key, None)) + '元' if str(row.get(key, None)) else '',
    'rateType': {
        'BF': '保本浮动收益',
        'FF': '非保本浮动收益'
    },
    'riskLevel': {
        '1': '低等风险',
        '2': '中低风险',
        '3': '中等风险',
        '4': '中高风险'
    },
    'saleStatus': {
        '1': '在售',
        '2': '存续'
    },
}
PC_REQUESTS_ITER = [
    # 代销产品-保险
    {
        'request': {
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'method': 'post',
            'data': lambda page_no: {
                'tableIndex': 'table08',
                'dataType': '08',
                'tplId': 'tpl08',
                'pageNum': f'{page_no}',
                'pageSize': 20,
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                        "accept-language": "zh-CN,zh;q=0.9",
                        "content-length": "99",
                        "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                        "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                        "origin": "https//ebank.pingan.com.cn",
                        "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                        },

        },
        'identifier': 'payh',
        'field_name_2_new_field_name': {
            'investmentScope': FIELD_MAPPINGS['投资性质'],
            'riskLevel': FIELD_MAPPINGS['风险等级'],
            'prdName': FIELD_MAPPINGS['产品名称'],
            'prdManager': FIELD_MAPPINGS['管理人'],
            'prdCode': FIELD_MAPPINGS['产品编码'],
            'status': FIELD_MAPPINGS['产品状态']
        },
        'check_props': ['logId', 'cpbm'],
        'title': '代销产品-保险'
    },
    # 代销产品-保险金信托
    {
        'request': {
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'method': 'post',
            'data': lambda page_no: {
                'tableIndex': 'table09',
                'dataType': '09',
                'tplId': 'tpl09',
                'pageNum': f'{page_no}',
                'pageSize': 20,
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                        "accept-language": "zh-CN,zh;q=0.9",
                        "content-length": "99",
                        "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                        "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                        "origin": "https//ebank.pingan.com.cn",
                        "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                        },

        },
        'identifier': 'payh',
        'field_value_mapping': {
            'minAmount': lambda x: str(x) + '元' if remove_space(str(x)) else '',
            'riskLevel': {
                '1': '低等风险',
                '2': '中低风险',
                '3': '中等风险',
                '4': '中高风险'
            }
        },
        'field_name_2_new_field_name': {
            'riskLevel': FIELD_MAPPINGS['风险等级'],
            'productName': FIELD_MAPPINGS['产品名称'],
            'productType': FIELD_MAPPINGS['投资性质'],
            'issuer': FIELD_MAPPINGS['管理人']
        },
        'check_props': ['logId', 'cpbm'],
        'title': '代销产品-保险金信托'
    },
    # 本行产品-对公结构性存款
    {
        'request': {
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'method': 'post',
            'data': lambda page_no: {
                'tableIndex': 'table10',
                'dataType': '10',
                'tplId': 'tpl10',
                'pageNum': f'{page_no}',
                'pageSize': 20,
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                        "accept-language": "zh-CN,zh;q=0.9",
                        "content-length": "99",
                        "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                        "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                        "origin": "https//ebank.pingan.com.cn",
                        "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                        },

        },
        'identifier': 'payh',
        'field_name_2_new_field_name': {
            'minAmount': FIELD_MAPPINGS['起购金额'],
            'riskLevel': FIELD_MAPPINGS['风险等级'],
            'prdName': FIELD_MAPPINGS['产品名称'],
            'manageBankName': FIELD_MAPPINGS['管理人'],
            'saleStatus': FIELD_MAPPINGS['产品状态'],
            'prdCode': FIELD_MAPPINGS['产品编码'],
            'rateType': FIELD_MAPPINGS['收益类型'],
            'taCode': FIELD_MAPPINGS['TA编码'],
            'tranBankSet': FIELD_MAPPINGS['销售渠道'],
            'productNo': FIELD_MAPPINGS['登记编码']
        },
        'check_props': ['logId', 'cpbm'],
        'title': '本行产品-对公结构性存款'
    },
    # 本行产品-公募基金
    {
        'request': {
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'method': 'post',
            'data': lambda page_no: {
                'tableIndex': 'table10',
                'dataType': '10',
                'tplId': 'tpl10',
                'pageNum': f'{page_no}',
                'pageSize': 20,
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                        "accept-language": "zh-CN,zh;q=0.9",
                        "content-length": "99",
                        "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                        "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                        "origin": "https//ebank.pingan.com.cn",
                        "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                        },

        },
        'identifier': 'payh',
        'field_value_mapping': {
            'minAmount': lambda x: str(x) + '元',
            'rateType': {
                'BF': '保本浮动收益',
                'FF': '非保本浮动收益'
            },
            'riskLevel': {
                '1': '低等风险',
                '2': '中低风险',
                '3': '中等风险',
                '4': '中高风险'
            },
            'saleStatus': {
                '1': '在售',
                '2': '存续',
                '5': '停止申购'
            },
        },
        'field_name_2_new_field_name': {
            'riskLevel': FIELD_MAPPINGS['风险等级'],
            'prdName': FIELD_MAPPINGS['产品名称'],
            'prdCode': FIELD_MAPPINGS['产品编码'],
            'prdArrName': FIELD_MAPPINGS['投资性质'],
            'taCode': FIELD_MAPPINGS['TA编码'],
            'taName': FIELD_MAPPINGS['TA名称'],
            'prdManager': FIELD_MAPPINGS['管理人'],
            'status': FIELD_MAPPINGS['产品状态']
        },
        'check_props': ['logId', 'cpbm'],
        'title': '本行产品-公募基金'
    },
    # 代销产品-信托/资管计划及其他
    {
        'request': {
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'method': 'post',
            'data': lambda page_no: {
                'tableIndex': 'table04',
                'dataType': '04',
                'tplId': 'tpl03',
                'pageNum': f'{page_no}',
                'pageSize': 20,
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                        "accept-language": "zh-CN,zh;q=0.9",
                        "content-length": "99",
                        "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                        "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                        "origin": "https//ebank.pingan.com.cn",
                        "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                        },

        },
        'identifier': 'payh',
        'field_value_mapping': {
            'minAmount': lambda x: str(x) + '元' if remove_space(str(x)) else '',
            'riskLevel': {
                '1': '低等风险',
                '2': '中低风险',
                '3': '中等风险',
                '4': '中高风险'
            }
        },
        'field_name_2_new_field_name': {
            'riskLevel': FIELD_MAPPINGS['风险等级'],
            'taCode': FIELD_MAPPINGS['TA编码'],
            'prdName': FIELD_MAPPINGS['产品名称'],
            'taName': FIELD_MAPPINGS['TA名称'],
            'prdManager': FIELD_MAPPINGS['管理人'],
            'prdCode': FIELD_MAPPINGS['产品编码']
        },
        'check_props': ['logId', 'cpbm'],
        'title': '代销产品-信托/资管计划及其他'
    },
    # 代销产品-理财子
    {
        'request': {
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'method': 'post',
            'data': lambda page_no: {
                'tableIndex': 'table02',
                'dataType': '02',
                'tplId': 'tpl02',
                'pageNum': f'{page_no}',
                'pageSize': 20,
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                        "accept-language": "zh-CN,zh;q=0.9",
                        "content-length": "99",
                        "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                        "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                        "origin": "https//ebank.pingan.com.cn",
                        "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                        },

        },
        'identifier': 'payh',
        'field_value_mapping': {
            'minAmount': lambda x: str(x) + '元' if remove_space(str(x)) else '',
            'riskLevel': {
                '1': '低等风险',
                '2': '中低风险',
                '3': '中等风险',
                '4': '中高风险'
            }
        },
        'field_name_2_new_field_name': {
            'riskLevel': FIELD_MAPPINGS['风险等级'],
            'prdName': FIELD_MAPPINGS['产品名称'],
            'saleStatus': FIELD_MAPPINGS['产品状态'],
            'prdCode': FIELD_MAPPINGS['产品编码'],
            'taCode': FIELD_MAPPINGS['TA编码'],
            'prdManager': FIELD_MAPPINGS['管理人'],
        },
        'check_props': ['logId', 'cpbm'],
        'title': '代销产品-理财子'
    },
    # 本行产品-个人结构性存款
    {
        'request': {
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'method': 'post',
            'data': lambda page_no: {
                'tableIndex': 'table06',
                'dataType': '06',
                'tplId': 'tpl06',
                'pageNum': f'{page_no}',
                'pageSize': 20,
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                        "accept-language": "zh-CN,zh;q=0.9",
                        "content-length": "99",
                        "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                        "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                        "origin": "https//ebank.pingan.com.cn",
                        "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                        },

        },
        'identifier': 'payh',
        'field_value_mapping': {
            'rateType': {
                'BF': '保本浮动收益',
                'FF': '非保本浮动收益',
            },
            'riskLevel': {
                '1': '低等风险',
                '2': '中低风险',
                '3': '中等风险',
                '4': '中高风险'
            },
            'saleStatus': {
                '1': '存续',
                '0': '在售',
                '5': '停止申购'
            },
        },
        'field_name_2_new_field_name': {
            'minAmount': FIELD_MAPPINGS['起购金额'],
            'rateType': FIELD_MAPPINGS['收益类型'],
            'riskLevel': FIELD_MAPPINGS['风险等级'],
            'prdName': FIELD_MAPPINGS['产品名称'],
            'saleStatus': FIELD_MAPPINGS['产品状态'],
            'prdCode': FIELD_MAPPINGS['产品编码'],
        },
        'check_props': ['logId', 'cpbm'],
        'title': '本行产品-个人结构性存款'
    },
    # 代销产品-养老保险
    {
        'request': {
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'method': 'post',
            'data': lambda page_no: {
                'tableIndex': 'table03',
                'dataType': '03',
                'tplId': 'tpl02',
                'pageNum': f'{page_no}',
                'pageSize': 20,
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                        "accept-language": "zh-CN,zh;q=0.9",
                        "content-length": "99",
                        "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                        "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                        "origin": "https//ebank.pingan.com.cn",
                        "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                        },

        },
        'identifier': 'payh',
        'field_value_mapping': {
            'status': {
                '1': '存续',
                '0': '在售',
                '5': '停止申购'
            },
            'riskLevel': {
                '1': '低等风险',
                '2': '中低风险',
                '3': '中等风险',
                '4': '中高风险'
            }
        },
        'field_name_2_new_field_name': {
            'riskLevel': FIELD_MAPPINGS['风险等级'],
            'prdName': FIELD_MAPPINGS['产品名称'],
            'prdCode': FIELD_MAPPINGS['产品编码'],
            'taCode': FIELD_MAPPINGS['TA编码'],
            'taName': FIELD_MAPPINGS['TA名称'],
            'prdManager': FIELD_MAPPINGS['管理人'],
            'status': FIELD_MAPPINGS['产品状态']
        },
        'check_props': ['logId', 'cpbm'],
        'title': '代销产品-养老保险'
    },
    # 本行产品-银行理财
    {
        'request': {
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'method': 'post',
            'data': lambda page_no: {
                'tableIndex': 'table01',
                'dataType': '01',
                'tplId': 'tpl01',
                'pageNum': f'{page_no}',
                'pageSize': 20,
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                        "accept-language": "zh-CN,zh;q=0.9",
                        "content-length": "99",
                        "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                        "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                        "origin": "https//ebank.pingan.com.cn",
                        "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                        },

        },
        'identifier': 'payh',
        'field_value_mapping': {
            'rateType': {
                'FF': '非保本浮动收益',
                'BF': '保本浮动收益'
            },
            'riskLevel': {
                '1': '低等风险',
                '2': '中低风险',
                '3': '中等风险',
                '4': '中高风险'
            },
            'saleStatus': {
                '0': '在售',
                '1': '存续'
            },
        },
        'field_name_2_new_field_name': {
            'minAmount': FIELD_MAPPINGS['起购金额'],
            'riskLevel': FIELD_MAPPINGS['风险等级'],
            'prdName': FIELD_MAPPINGS['产品名称'],
            'saleStatus': FIELD_MAPPINGS['产品状态'],
            'prdCode': FIELD_MAPPINGS['产品编码'],
            'productNo': FIELD_MAPPINGS['登记编码'],
            'rateType': FIELD_MAPPINGS['收益类型']
        },
        'check_props': ['logId', 'cpbm'],
        'title': '本行产品-银行理财'
    },
    # 本行产品-银行公司理财
    {
        'request': {
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'method': 'post',
            'data': lambda page_no: {
                'tableIndex': 'table05',
                'dataType': '05',
                'tplId': 'tpl03',
                'pageNum': f'{page_no}',
                'pageSize': 20,
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                        "accept-language": "zh-CN,zh;q=0.9",
                        "content-length": "99",
                        "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                        "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                        "origin": "https//ebank.pingan.com.cn",
                        "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                        },

        },
        'identifier': 'payh',
        'field_value_mapping': {
            'minAmount': lambda x: str(x) + '元',
            'riskLevel': {
                '1': '低等风险',
                '2': '中低风险',
                '3': '中等风险',
                '4': '中高风险'
            }
        },
        'field_name_2_new_field_name': {
            'minAmount': FIELD_MAPPINGS['起购金额'],
            'riskLevel': FIELD_MAPPINGS['风险等级'],
            'prdName': FIELD_MAPPINGS['产品名称'],
            'manageBankName': FIELD_MAPPINGS['管理人'],
            'saleStatus': FIELD_MAPPINGS['产品状态'],
            'prdCode': FIELD_MAPPINGS['产品编码'],
            'rateType': FIELD_MAPPINGS['收益类型'],
            'taCode': FIELD_MAPPINGS['TA编码'],
            'tranBankSet': FIELD_MAPPINGS['销售渠道'],
            'productNo': FIELD_MAPPINGS['登记编码'],
        },
        'check_props': ['logId', 'cpbm'],
        'title': '本行产品-银行公司理财'
    },
    # 代销产品-公司代销理财
    {
        'request': {
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'method': 'post',
            'data': lambda page_no: {
                'tableIndex': 'table11',
                'dataType': '11',
                'tplId': 'tpl11',
                'pageNum': f'{page_no}',
                'pageSize': 20,
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                        "accept-language": "zh-CN,zh;q=0.9",
                        "content-length": "99",
                        "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                        "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                        "origin": "https//ebank.pingan.com.cn",
                        "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                        "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                        },

        },
        'identifier': 'payh',
        'field_value_mapping': {
            'minAmount': lambda x: str(x) + '元' if remove_space(str(x)) else '',
            'riskLevel': {
                '1': '低等风险',
                '2': '中低风险',
                '3': '中等风险',
                '4': '中高风险'
            }
        },
        'field_name_2_new_field_name': {
            'minAmount': FIELD_MAPPINGS['起购金额'],
            'riskLevel': FIELD_MAPPINGS['风险等级'],
            'prdName': FIELD_MAPPINGS['产品名称'],
            'manageBankName': FIELD_MAPPINGS['管理人'],
            'saleStatus': FIELD_MAPPINGS['产品状态'],
            'prdCode': FIELD_MAPPINGS['产品编码'],
            'rateType': FIELD_MAPPINGS['收益类型'],
            'taCode': FIELD_MAPPINGS['TA编码'],
            'tranBankSet': FIELD_MAPPINGS['销售渠道'],
            'productNo': FIELD_MAPPINGS['登记编码']
        },
        'check_props': ['logId', 'cpbm'],
        'title': '代销产品-公司代销理财'
    },
]
