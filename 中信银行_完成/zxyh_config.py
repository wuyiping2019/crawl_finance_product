from crawl_utils.mappings import FIELD_MAPPINGS

MASK = 'zxyh'
LOG_NAME = '中信银行'

PC_REQUEST_URL = 'https://m1.cmbc.com.cn/gw/m_app/FinFundProductList.do'
PC_REQUEST_METHOD = 'POST'
PC_REQUEST_JSON = lambda page_no: {
    "request": {
        "header": {
            "appId": "",
            "appVersion": "web",
            "appExt": "999",
            "device":
                {
                    "appExt": "999",
                    "osType": "03",
                    "osVersion": "",
                    "uuid": ""
                }
        },
        "body": {
            "pageSize": 10,
            "currentIndex": (page_no - 1) * 10,
            "startId": (page_no - 1) * 10 + 1,
            "prdTypeNameList": [],
            "sellingListTypeIndex": None,
            "sortFileldName": "LIQU_MODE_NAME",
            "sortFileldType": "DESC",
            "prdChara": "",
            "liveTime": "",
            "pfirstAmt": "",
            "currType": "",
            "keyWord": "",
            "isKJTSS": "0"
        }
    }
}
PC_REQUEST_HEADERS = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br",
                      "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", "Connection": "keep-alive", "Content-Length": "377",
                      "Content-Type": "application/json;charset=UTF-8",
                      "Host": "m1.cmbc.com.cn", "Origin": "https//m1.cmbc.com.cn",
                      "Referer": "https//m1.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/fund-commission/prd-list",
                      "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                      "sec-ch-ua-mobile": "?1", "sec-ch-ua-platform": "\"Android\"", "Sec-Fetch-Dest": "empty",
                      "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                      "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36",
                      "X-Tingyun": "c=B|MbVbeLGiVew;x=804e30a865b14a1d"}
PC_FIELD_NAME_2_NEW_FIELD_NAME = {
    'PRD_NAME': FIELD_MAPPINGS['产品名称'],
    'CHANNELS_NAME': FIELD_MAPPINGS['销售渠道'],
    'NAV': FIELD_MAPPINGS['净值'],
    'NAV_DATE': FIELD_MAPPINGS['净值日期'],
    'PRD_CODE': FIELD_MAPPINGS['产品编码'],
    'DIV_MODES_NAME': FIELD_MAPPINGS['分红方式'],
    'RISK_LEVEL_NAME': FIELD_MAPPINGS['风险等级'],
    'STATUS_NAME': FIELD_MAPPINGS['产品状态'],
    'CYCLE_DAYS': FIELD_MAPPINGS['投资期限'],
    'IPO_START_DATE': FIELD_MAPPINGS['募集起始日期'],
    'IPO_END_DATE': FIELD_MAPPINGS['募集结束日期'],
    'PRD_MANAGER_NAME': FIELD_MAPPINGS['管理人'],
    'PRD_MANAGER': FIELD_MAPPINGS['管理人编码'],
    'PRD_TRUSTEE_NAME': FIELD_MAPPINGS['委托人'],
    'PRD_TRUSTEE': FIELD_MAPPINGS['委托人编码'],
    'TA_CODE': FIELD_MAPPINGS['TA编码'],
    'TA_NAME': FIELD_MAPPINGS['TA名称']
}
