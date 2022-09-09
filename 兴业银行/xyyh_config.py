LOG_NAME = '兴业银行'
MASK_STR = 'xyyh'
SEQUENCE_NAME = f'seq_{MASK_STR}'
TRIGGER_NAME = f'trigger_{MASK_STR}'
TARGET_TABLE_PROCESSED = f'ip_bank_{MASK_STR}_processed'
STR_TYPE = 'clob'
NUMBER_TYPE = 'number(11)'

# 休眠时间
SLEEP_SECOND = 3

# PC-本行理财产品
PC_BHLCCP_REQUEST = {
    'url': 'https://www.cib.com.cn/cn/personal/wealth-management/xxcx/table/',
    'method': 'get',
    'headers': {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": "www.cib.com.cn",
        "Pragma": "no-cache",
        "Referer": "https//www.cib.com.cn/cn/personal/wealth-management/xxcx/pfund.html",
        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": "\"Android\"",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36"
    },
    'data': None

}


