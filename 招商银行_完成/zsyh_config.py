from crawl_utils.mappings import FIELD_MAPPINGS

MASK = 'zsyh'
LOG_NAME = '招商银行'

PC_REQUEST_URL = 'http://www.cmbchina.com/cfweb/svrajax/product.ashx'
PC_REQUEST_PARAMS = lambda page_no: {
    'op': 'search',
    'type': 'm',
    'pageindex': f'{page_no}',
    'salestatus': '',
    'baoben': '',
    'currency': '',
    'term': '',
    'keyword': '',
    'series': '01',
    'risk': '',
    'city': '',
    'date': '',
    'pagesize': '20',
    'orderby': 'ord1',
    't': '0.037500909552102835',
    'citycode': '',
}
PC_REQUEST_HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Cookie": "CWF.AUTH=CLNO=001376ACGtn3101122&SLNO=VgHP05cAu6mRlaRzodoKWHzE; browsehistory=%7B%22titlearray%22%3A%5B%22%20%20%u4E2A%u4EBA%u7406%u8D22%u4EA7%u54C1%22%2C%22%20%20%u4E2A%u4EBA%u7406%u8D22%u4EA7%u54C1%22%5D%7D",
    "Host": "www.cmbchina.com",
    "Pragma": "no-cache",
    "Referer": "http//www.cmbchina.com/cfweb/Personal/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
}
PC_FIELD_NAME_2_NEW_FIELD_NAME = {
    'PrdCode': FIELD_MAPPINGS['产品编码'],
    'PrdName': FIELD_MAPPINGS['产品名称'],
    'PrdBrief': FIELD_MAPPINGS['产品简称'],
    'funCod': FIELD_MAPPINGS['产品编码'],
    'AreaCode': FIELD_MAPPINGS['销售区域'],
    'Currency': FIELD_MAPPINGS['币种'],
    'BeginDate': FIELD_MAPPINGS['募集起始日期'],
    'EndDate': FIELD_MAPPINGS['募集结束日期'],
    'ShowExpireDate': FIELD_MAPPINGS['产品到期日'],
    'Status': FIELD_MAPPINGS['产品状态'],
    'NetValue': FIELD_MAPPINGS['净值'],
    'Term': FIELD_MAPPINGS['投资期限'],
    'InitMoney': FIELD_MAPPINGS['起购金额'],
    'Risk': FIELD_MAPPINGS['风险等级'],
    'SaleChannelName': FIELD_MAPPINGS['销售渠道'],
    'ShowExpectedReturn': FIELD_MAPPINGS['业绩比较基准'],
    'REGCode': FIELD_MAPPINGS['登记编码'],
    'Style': FIELD_MAPPINGS['投资性质']
}
