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

MOBILE_REQUEST_URL = 'https://wap.spdb.com.cn/mspmk-web-financechannel/QueryFinanceList.hh'
MOBILE_REQUEST_METHOD = 'POST'
MOBILE_REQUEST_DATA = lambda page_no: {
    "ReqHeader": {"AlgoType": "1", "MarketingSession": "\"\"",
                  "DeviceInfo": "WEB_MAPPING_deviceID|iPhone|iPhoneWEB_MAPPING_buildType",
                  "DeviceDigest": "WEB_MAPPING_getDeviceDigest", "NetWorkType": "1",
                  "NetWorkName": "WEB_MAPPING_getNetWorkName", "AppVersion": "",
                  "AppDeviceType": "WEB_MAPPING_deviceBrand WEB_MAPPING_deviceModel",
                  "AppExVersion": ""},
    "ReqBody": {"SegmentFlag": "0", "BeginNumber": f"{(page_no - 1) * 10}", "QueryNumber": "10", "MobileNo": "",
                "Finance_SelectType": "", "Finance_Sort": "0", "Finance_SortFlag": "0",
                "flag": "viewAppearState"}
}
MOBILE_REQUEST_HEADERS = {
    "Accept": "application/json", "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "Cache-Control": "no-cache", "Connection": "keep-alive",
    "Content-Type": "application/json",
    # "Cookie": "mobwapGsessionid=151DD8BA8388611ED934AB3D5B29BAC4D; mobwapLbanGsessionid=0BFA0023E38AF11ED9DDB295B4ED64495; msJsessionid=4C4EB205BDDE94ACB318050EECD70705",
    # "Host": "wap.spdb.com.cn", "Origin": "https//wap.spdb.com.cn",
    "Pragma": "no-cache",
    "Referer": "https//wap.spdb.com.cn/mspmk-cli-financechannel/financeCommonList?H5Channel=400",
    "sec-ch-ua": "\"Microsoft Edge\";v=\"105\", \" Not;A Brand\";v=\"99\", \"Chromium\";v=\"105\"",
    "sec-ch-ua-mobile": "?1", "sec-ch-ua-platform": "\"Android\"",
    "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Mobile Safari/537.36 Edg/105.0.1343.42"
}

MOBILE_DETAIL_REQUEST_URL = 'https://wap.spdb.com.cn/mspmk-web-finance/QueryFinanceProduct.hh'
MOBILE_DETAIL_REQUEST_METHOD = 'POST'
MOBILE_DETAIL_REQUEST_HEADERS = {
    "Accept": "application/json", "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9", "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Content-Type": "application/json; charset=UTF-8",
    "Cookie": "mobwapGsessionid=18845A429388311EDB93195319C98A94A; mobwapLbanGsessionid=1FCEAF0CA388311EDB799B9725AADA930; msJsessionid=24C12AF771698544693F496713EA44B5",
    "Pragma": "no-cache",
    "Referer": "https//wap.spdb.com.cn/mspmk-cli-financebuy/productDetails?FinanceNo=2301222290&H5Channel=400&FromUrl=%2Fmspmk-cli-financechannel%2FfinanceCommonList%3FH5Channel%3D400",
    "sec-ch-ua": "\"Google Chrome\";v=\"105\", \"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"105\"",
    "sec-ch-ua-mobile": "?1", "sec-ch-ua-platform": "\"Android\"",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Mobile Safari/537.36"
}
MOBILE_DETAIL_REQUEST_DATA = lambda Finance_No: {
    "ReqHeader": {
        "AlgoType": "1",
        "MarketingSession": "\"\"",
        "DeviceInfo": "WEB_MAPPING_deviceID|iPhone|iPhoneWEB_MAPPING_buildType",
        "DeviceDigest": "WEB_MAPPING_getDeviceDigest",
        "NetWorkType": "1",
        "NetWorkName": "WEB_MAPPING_getNetWorkName",
        "AppVersion": "",
        "AppDeviceType": "WEB_MAPPING_deviceBrand WEB_MAPPING_deviceModel",
        "AppExVersion": ""
    },
    "ReqBody": {
        "FinanceNo": Finance_No,
        "MobileNo": ""
    }
}
MOBILE_FIELD_NAME_2_NEW_FIELD_NAME = {
    'Finance_No': FIELD_MAPPINGS['产品编码'],
    'FinanceSupplementAmnt': FIELD_MAPPINGS['递增金额'],
    'FinanceStartDate': FIELD_MAPPINGS['发售起始日期'],
    'FinanceAbbrName': FIELD_MAPPINGS['产品简称'],
    'FinanceCurrency': FIELD_MAPPINGS['币种'],
    'FinanceFoundedDate': FIELD_MAPPINGS['成立日'],
    'FinanceName': FIELD_MAPPINGS['产品名称'],
    'FinanceEarningsFlag': FIELD_MAPPINGS['起购金额'],
    'FinanceIssuer': FIELD_MAPPINGS['风险等级'],
    'FinanceEndDate': FIELD_MAPPINGS['发售截止日期'],
    'FinanceValidityDesc': FIELD_MAPPINGS['投资期限'],
    'FinanceRedemptionRule': FIELD_MAPPINGS['递增规则'],
    'FinanceTransStartRule': FIELD_MAPPINGS['起息规则'],

}
# MOBILE_FIELD_NAME_2_NEW_FIELD_NAME = {
#     'FinanceExpireDate': result['FinanceExpireDate'],
#     'FinanceKind': result['FinanceKind'],
#     'CurrentDate': result['CurrentDate'],
#     'HasHoldings': result['HasHoldings'],
#     'RegistrantNo': result['RegistrantNo'],
#     'FinanceBenchmarkDesc': result['FinanceBenchmarkDesc'],
#     'ChartTips': result['ChartTips'],
#     'EVoucherCodeMaxAmnt': result['EVoucherCodeMaxAmnt'],
#     'EndEntryRule': result['EndEntryRule'],
#     'InvestmentOrientation': result['InvestmentOrientation'],
#     'FinanceFastRedemptionRule': result['FinanceFastRedemptionRule'],
#     'IsFavorite': result['IsFavorite'],
#     'EVoucherCodeMinAmnt': result['EVoucherCodeMinAmnt'],
#     'FinanceRedemptionStartDate': result['FinanceRedemptionStartDate'],
#     'FinanceReturnRateName': result['FinanceReturnRateName'],
#     'FinanceBrand': result['FinanceBrand'],
#     'FinanceTransStage': result['FinanceTransStage'],
#     'FinanceBenchmark': result['FinanceBenchmark'],
#     'EndRule': result['EndRule'],
#     'syqsrq': result['FinanceEarningsStartDate'],
#     'RiskAlert': result['RiskAlert'],
#     'ProductSegmentFlag': result['ProductSegmentFlag'],
#     'FinanceReturnRateLevel': result['FinanceReturnRateLevel'],
#     # 'FinanceShareLevel': result['FinanceShareLevel'],
#     'FinanceEarningsType': result['FinanceEarningsType'],
#     'FinanceShowYieldEstimation': result['FinanceShowYieldEstimation'],
#     'FinanceRedemptionFee': result['FinanceRedemptionFee'],
#     'DigitalHuman': result['DigitalHuman'],
#     'FinancePurchaserNum': result['FinancePurchaserNum'],
#     'cpbm': result['FinanceNo'],
#     'FinanceReturnRate': result['FinanceReturnRate'],
#     'fxdj': result['FinanceRiskLevel'],
#     'AssetMgmtIntro': result['AssetMgmtIntro'],
#     'FinanceType': result['FinanceType'],
#     'PerformanceRewardsDesc': result['PerformanceRewardsDesc'],
#     'shgz': result['FinanceRedemptionStartTime'],
#     'FinanceFastRedemptionMaxAmnt': result['FinanceFastRedemptionMaxAmnt'],
#     'FinanceValidity': result['FinanceValidity'],
#     'FinanceMinRedemption': result['FinanceMinRedemption'],
#     'DefaultTab': result['DefaultTab'],
#     # 'MovementsType': result['MovementsType'],
#     'FinanceExpectedYield': result['FinanceExpectedYield']
# }
