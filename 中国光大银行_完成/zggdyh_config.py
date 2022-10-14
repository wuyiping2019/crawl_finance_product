import re

from crawl_utils.mappings import FIELD_MAPPINGS

MASK = 'zggdyh'
MARK_STR = 'zggdyh'
LOG_NAME = '中国光大银行'
SEQUENCE_NAME = f'seq_{MARK_STR}'
TRIGGER_NAME = f'trigger_{MARK_STR}'
CRAWL_REQUEST_DETAIL_TABLE = f'crawl_{MARK_STR}'

TARGET_TABLE_PROCESSED = f'ip_bank_{MARK_STR}_processed'
STR_TYPE = 'clob'
NUMBER_TYPE = 'number(11)'
SLEEP_SECOND = 1
PATTERN_Z = re.compile(r'[(](.*)[)]', re.S)
PATTERN_E = re.compile(r'[（](.*)[）]', re.S)

PC_REQUEST_URL = 'http://www.cebbank.com/eportal/ui'
PC_REQUEST_METHOD = 'POST'
PC_REQUEST_PARAMS = {
    'moduleId': 12073,
    'struts.portlet.action': '/app/yglcAction!listProduct.action'
}
PC_REQUEST_DATA = lambda \
        page_no: f'codeOrName=&TZBZMC=&SFZS=&qxrUp=Y&qxrDown=&dqrUp=&dqrDown=&qdjeUp=&qdjeDown=&qxUp=&qxDown=&yqnhsylUp=&yqnhsylDown=&page={page_no}&pageSize=5&channelIds%5B%5D=yxl94&channelIds%5B%5D=ygelc79&channelIds%5B%5D=hqb30&channelIds%5B%5D=dhb2&channelIds%5B%5D=cjh&channelIds%5B%5D=gylc70&channelIds%5B%5D=Ajh67&channelIds%5B%5D=Ajh84&channelIds%5B%5D=901776&channelIds%5B%5D=Bjh91&channelIds%5B%5D=Ejh99&channelIds%5B%5D=Tjh70&channelIds%5B%5D=tcjh96&channelIds%5B%5D=ts43&channelIds%5B%5D=ygjylhzhMOM25&channelIds%5B%5D=yxyg87&channelIds%5B%5D=zcpzjh64&channelIds%5B%5D=wjyh1&channelIds%5B%5D=smjjb9&channelIds%5B%5D=ty90&channelIds%5B%5D=tx16&channelIds%5B%5D=ghjx6&channelIds%5B%5D=ygxgt59&channelIds%5B%5D=wbtcjh3&channelIds%5B%5D=wbBjh77&channelIds%5B%5D=wbTjh28&channelIds%5B%5D=sycfxl&channelIds%5B%5D=cfTjh&channelIds%5B%5D=jgdhb&channelIds%5B%5D=tydhb&channelIds%5B%5D=jgxck&channelIds%5B%5D=jgyxl&channelIds%5B%5D=tyyxl&channelIds%5B%5D=dgBTAcp&channelIds%5B%5D=27637097&channelIds%5B%5D=27637101&channelIds%5B%5D=27637105&channelIds%5B%5D=27637109&channelIds%5B%5D=27637113&channelIds%5B%5D=27637117&channelIds%5B%5D=27637121&channelIds%5B%5D=27637125&channelIds%5B%5D=27637129&channelIds%5B%5D=27637133&channelIds%5B%5D=gyxj32&channelIds%5B%5D=yghxl&channelIds%5B%5D=ygcxl&channelIds%5B%5D=ygjxl&channelIds%5B%5D=ygbxl&channelIds%5B%5D=ygqxl&channelIds%5B%5D=yglxl&channelIds%5B%5D=ygzxl&channelIds%5B%5D=ygttg'
PC_REQUEST_HEADERS = {"Accept": "text/html, */*; q=0.01", "Accept-Encoding": "gzip, deflate",
                      "Accept-Language": "zh-CN,zh;q=0.9", "Cache-Control": "no-cache", "Connection": "keep-alive",
                      "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                      "Cookie": "iss_svid=3db483635f3118cce60781c37d640a2e; BIGipServerpool_nport=!SoROrs9fjqvwln7sBoHfVu2SmXZmucSQJHn8C09BNXNhOCpe4QNLbB8TRa6tezR6L07bcId4d5nEHA==; BIGipServerpool_waf_nport=!HlOHoSB32zKEXs7sBoHfVu2SmXZmuXt6d0A7LPOGouClENMLdVd1jZXk8TVh+vi8L+s/7gMCad7W; iss_nu=0; iss_cc=true; iss_sid=4c8d1fed3be6c9bfd366f00e2f58541d; iss_id=df88ff4b8d669b2d6dc5f57fbc27541c; iss_ot=1665543117898; iss_re=body///*%5B@id%3D%22limitCATEGORY%22%5D-javascript%3Avoid%280%29",
                      "Host": "www.cebbank.com", "Origin": "http//www.cebbank.com", "Pragma": "no-cache",
                      "Referer": "http//www.cebbank.com/site/gryw/yglc/lccp49/index.html",
                      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
                      "X-Requested-With": "XMLHttpRequest"}
PC_FIELD_NAME_2_NEW_FIELD_NAME = {
    '产品名称': FIELD_MAPPINGS['产品名称'],
    '币种': FIELD_MAPPINGS['币种'],
    '发售起始日期': FIELD_MAPPINGS['发售起始日期'],
    '发售截止日期': FIELD_MAPPINGS['发售截止日期'],
    '起点金额': FIELD_MAPPINGS['起购金额'],
    '业绩比较基准': FIELD_MAPPINGS['业绩比较基准'],
    '产品编码': FIELD_MAPPINGS['产品编码'],
    '递增金额': FIELD_MAPPINGS['递增金额'],
    '开放日': FIELD_MAPPINGS['开放日'],
    '产品类型': FIELD_MAPPINGS['运作模式'],
    '剩余额度': FIELD_MAPPINGS['剩余额度'],
    '产品状态': FIELD_MAPPINGS['产品状态']
}
