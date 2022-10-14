import json

from crawl_utils.mappings import FIELD_MAPPINGS

MASK = 'pfyh'
LOG_NAME = '浦发银行'

PC_REQUEST_URL = 'https://per.spdb.com.cn/was5/web/search'
PC_REQUEST_METHOD = 'POST'
PC_REQUEST_DATA = lambda page_no: {
    "metadata": "ProductCode|ProductName|DeadlineBrandiD|IncomeDates|RiskLevel|CurrencyType|IndiiPOMinAmnt|TACode|TAName|ChannelDisincomeRate|IncomeRateDes|Productstatus",
    "channelid": 267467,
    "page": page_no,
    "searchword": "(DeadlineBrandiD=%)*(IncomeDates=%)*(RiskLevel=%)*(CurrencyType=%)*(IndiIPOMinStauts=%)*(TACode=%)*(ProductStatus='\u5728\u552e' or ProductStatus='\u505c\u552e')*(ProductName!=\u516c\u53f8%)*(ProductName!=\u540c\u4e1a%)*(ProductName!=\u5229\u591a\u591a%)*(ProductName!=\u8d22\u5bcc\u73ed\u8f66%)*(ProductName!=\u5929\u6dfb\u5229%)*(ProductName!=\u542f\u94ed%)*(ProductName!=\u5929\u901a\u5229%)*(ProductName!=\u4fe1\u6258%)*(ProductName!=\u4e0a\u4fe1%)*(ProductCode!=2301137335)*(ProductCode!=2301212262)*(ProductCode!=2301228801)*(ProductCode!=2301228803)*(ProductCode!=2301174902)*(ProductCode!=2301174903)*(ProductCode!=2301157376)*(ProductCode!=2301157377)*(ProductCode!=2301229055)"
}

PC_REQUEST_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Host": "per.spdb.com.cn",
    "Origin": "https//per.spdb.com.cn",
    "Pragma": "no-cache",
    "Referer": "https//per.spdb.com.cn/bank_financing/financial_product/",
    "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}

PC_FIELD_VALUE_MAPPING = {
    'yjbjjz': lambda row, key: (key, json.dumps(
        {'title': row['IncomeRateDes'], 'value': row['ChannelDisincomeRate']}).encode().decode('unicode_escape'))
}

PC_FIELD_NAME_2_NEW_FIELD_NAME = {
    'ProductName': FIELD_MAPPINGS['产品名称'],
    'ProductCode': FIELD_MAPPINGS['产品编码'],
    'yjbjjz': FIELD_MAPPINGS['业绩比较基准'],
    'IncomeDates': FIELD_MAPPINGS['投资期限'],
    'TAName': FIELD_MAPPINGS['TA名称'],
    'RiskLevel': FIELD_MAPPINGS['风险等级'],
    'CurrencyType': FIELD_MAPPINGS['币种'],
    'TACode': FIELD_MAPPINGS['TA编码'],

}
