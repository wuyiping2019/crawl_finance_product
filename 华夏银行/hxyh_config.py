import json

import requests

from utils.common_utils import extract_content_between_content
LOG_NAME = '华夏银行'
MASK_STR = 'hxyh'
SEQUENCE_NAME = f'seq_{MASK_STR}'
TRIGGER_NAME = f'trigger_{MASK_STR}'
TARGET_TABLE_PROCESSED = f'ip_bank_{MASK_STR}_processed'
STR_TYPE = 'clob'
NUMBER_TYPE = 'number(11)'
SLEEP_SECOND = 3
HXYH_PC_REQUEST = {
    'url': lambda
        page_num,
        page_size=40: f'https://www.hxb.com.cn/precious-metal-mall/LchqController/findPage?pageNum={page_num}&pageSize={page_size}',
    'headers': {"Accept": "application/json, text/javascript, */*; q=0.01", "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", "Cache-Control": "no-cache", "Connection": "keep-alive",
                "Content-Length": "105", "Content-Type": "application/json;charset=UTF-8", "Host": "www.hxb.com.cn",
                "Origin": "https//www.hxb.com.cn", "Pragma": "no-cache",
                "Referer": "https//www.hxb.com.cn/grjr/lylc/zzfsdlccpxx/index.shtml",
                "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"", "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest"},
    'data': {"licaiName": "", "licaiBuyOriginOrderFlag": "", "licaiVieldMaxOrderFlag": "",
             "licaiTimeLimitMaxOrderFlag": ""},
    'method': 'post',
    'cpsms': {
        'headers': {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate, br", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache", "Connection": "keep-alive", "Host": "www.hxb.com.cn", "Pragma": "no-cache",
            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"", "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Site": "none", "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"}
    }
}
HXYH_MOBILE_REQUEST = {
    'prd_list': {
        'url': ' https://m.hxb.com.cn/wechat/wxbank/finance/finance/netValueFinancial',
        'headers': {"Accept-Encoding": "gzip, deflate, br",
                    "Referer": "https://m.hxb.com.cn/wechat/static/index.html?code=041JnAGa100mUD0NPCJa1IAcNr1JnAGX&state=STATE",
                    "Connection": "keep-alive", "Host": "m.hxb.com.cn", "BL": "null",
                    "User-Agent": "Mozilla\/5.0 (iPhone; CPU iPhone OS 15_6_1 like Mac OS X) AppleWebKit\/605.1.15 (KHTML, like Gecko)  Mobile\/15E148 wxwork\/4.0.16 MicroMessenger\/7.0.1 Language\/zh ColorScheme\/Light",
                    "appid": "1606292992775",
                    "Accept-Language": "zh-CN,zh-Hans;q=0.9",
                    "Accept": "application/json, text/plain, */*",
                    "rev": "DGTwR91tAos2LmB/8vZ+J+PINLDBOgiXe7XGl1AzoDN0s8njrrMD53BSA1INZ5o1N/fAAuUBp1y/lGv59ZEGngwF/Wi+HXTUmYLmM09rIbfMRcZDbdwzCbuprGA23sWTRZW5T/OTLeLIl6b14WRuYL8XmHC9qNmhtMzV73xtJG7J3E66hgRg3jz+z3/oSy/jqYhEKKPQfUwoNV5JO34xcO2ic9c2hQdIcGE0TsUTxJWtX6lvBiba095/pzj38kw5XBg2M99iyKoE4EciulK8Pl0UjDq0Bx2r761BJZGOvHTwHeGxfI9v0MzSNE6wlXJC/dAM4tcGbAZc0k3kyCegVQ=="
                    },
        'data': lambda page=1: {
            "request": {
                "sortField": "",
                "sortType": "",
                "type": "",
                "productName": "",
                "page": page,
                "hidderLoading": True
            }
        },
        'method': 'post'
    },
    'prd_signature': {
        'method': 'post',
        'url': 'https://m.hxb.com.cn/wechat/wxbank/base1/base/createWeixinSignatureService',
        'headers': {
            "Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh-Hans;q=0.9", "BL": "null", "Connection": "keep-alive",
            "Content-Length": "126", "Content-Type": "application/json;charset=UTF-8", "Host": "m.hxb.com.cn",
            "Origin": "https//m.hxb.com.cn",
            # "Referer": "https//m.hxb.com.cn/wechat/static/index.html?code=071KZo000gBYyO1MU92002PVW02KZo0J&state=STATE",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 15_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6.1 Mobile/15E148 Safari/604.1",
            # "appid": "1606292992775",
            "rev": ""},
        'data': {
            "request": {
                "signatureUrl": "https://m.hxb.com.cn/wechat/static/index.html?code=071KZo000gBYyO1MU92002PVW02KZo0J&state=STATE"
            }
        }

    },
    'prd_detail': {
        'headers': lambda refer, appid: {
            "Content-Length": "73",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 15_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6.1 Mobile/15E148 Safari/604.1",
            "Referer": refer,
            "Host": "m.hxb.com.cn",
            "Content-Type": "application/json;charset=UTF-8",
            "appid": appid,
            "BL": "null",
            "Accept-Language": "zh-CN,zh-Hans;q=0.9",
            # "Cookie": "LSESSION=YTZjYTFlYTctMmZjMi00ZjM1LTlkYjktNmNmOTY2NjJlY2Vh",
            "Origin": "https://m.hxb.com.cn",
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "application/json, text/plain, */*",
            "rev": ""
        },
        'url': 'https://m.hxb.com.cn/wechat/wxbank/finance/finance/netValueFinancialDetail',
        'data': lambda product_code: {
            'request': {
                'channel': '1',
                'productCode': product_code,
                'managercode': ''
            }
        },
        'method': 'post'
    }
}

if __name__ == '__main__':
    session = requests.session()
    try:
        response = session.request(method=HXYH_MOBILE_REQUEST['prd_signature']['method'],
                                   url=HXYH_MOBILE_REQUEST['prd_signature']['url'],
                                   json=HXYH_MOBILE_REQUEST['prd_signature']['data'],
                                   headers=HXYH_MOBILE_REQUEST['prd_signature']['headers'])
        loads = json.loads(
            response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text)
        loads_resp = loads['response']
        if isinstance(loads_resp, str):
            loads_resp = json.loads(loads_resp)
        elif isinstance(loads_resp, dict):
            print(loads_resp)
        print("===========")
        print(loads_resp)
        print("===============")
        cookies = requests.utils.dict_from_cookiejar(response.cookies)
        response_detail = session.request(method=HXYH_MOBILE_REQUEST['prd_detail']['method'],
                                          url=HXYH_MOBILE_REQUEST['prd_detail']['url'],
                                          json=HXYH_MOBILE_REQUEST['prd_detail']['data']('208212100401'),
                                          headers=HXYH_MOBILE_REQUEST['prd_detail']['headers'](
                                              refer=loads_resp['data']['url'],
                                              appid=loads_resp['data']['appId']
                                          ),
                                          cookies=cookies)
        print(response_detail.text)
    except Exception as e:
        print(e)
    finally:
        session.close()
