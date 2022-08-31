import requests
import json
import math
from bs4 import BeautifulSoup
from requests import Session

from utils.db_utils import close, get_conn_oracle
from utils.mark_log import mark_failure_log, getLocalDate, insertLogToDB
from utils.spider_flow import process_flow

TAGET_TABLE = 'ip_zglcw_bank'
LOG_NAME = '中国理财网'
City= {
    '000000':'全国','820000':'澳门','340000':'安徽','110000':'北京','500000':'重庆','210200':'大连','350000':'福建','620000':'甘肃','440000':'广东','450000':'广西','520000':'贵州',
    '410000':'河南','130000':'河北','430000':'湖南','420000':'湖北','230000':'黑龙江','460000':'海南','220000':'吉林','210000':'辽宁','320000':'江苏','360000':'江西','330200':'宁波',
    '150000':'内蒙古','640000':'宁夏','310000':'上海','370000':'山东','140000':'山西','440300':'深圳','510000':'四川','370200':'青岛','630000':'青海','610000':'陕西','900000':'其他国家或地区',
    '120000':'天津','710000':'台湾','330000':'浙江','350200':'厦门','530000':'云南','650000':'新疆','810000':'香港','540000':'西藏'
}

HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Length': '157',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Host': 'www.chinawealth.com.cn',
    'Origin': 'https://www.chinawealth.com.cn',
    'Pragma': 'no-cache',
    'Referer': 'https://www.chinawealth.com.cn/zzlc/jsp/lccp.jsp',
    'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

TYPES = [
    {
        'url': 'https://www.chinawealth.com.cn/LcSolrSearch.go',
        'method': 'post',
        'requestParams': {
            'cpjglb':'',
            'cpyzms':'',
            'cptzxz':'',
            'cpfxdj':'',
            'cpqx':'',
            'mjbz':'',
            'cpzt': '02,04,06',
            'mjfsdm': '01,NA',
            'cpdjbm':'',
            'cpmc':'',
            'cpfxjg':'',
            'yjbjjzStart':'',
            'yjbjjzEnd':'',
            'areacode':'',
            'pagenum':'1',
            'orderby':'',
            'code':'',
            'sySearch': '-1',
            'changeTableFlage': '0'
        }
    }
]


def process_spider_detail(conn, cursor, session: Session, log_id: int, **kwargs):
    """
    处理爬虫细节的回调函数
    :param conn:自动传参
    :param cursor: 自动传参
    :param session: 自动传参
    :param log_id: 自动传参
    :param kwargs: 额外需要的参数 手动传入
    :return:
    """
    for type in TYPES:
        request_url = type['url']
        request_method = type['method']
        request_params = type['requestParams']
        resp = session.request(method=request_method, url=request_url,data=request_params, headers=HEADERS).text
        # 获取总页数
        total_page = get_total_page(resp)
        # 循环处理每页数据
        loop_pages(total_page, session, cursor, log_id, request_url, request_method, request_params)
        break


def process_one_page(session: Session,
                     cur,
                     log_id: int,
                     request_url: str,
                     request_method: str,
                     request_params: dict):
    """
    处理爬取的一页数据并写入数据库
    :param session:
    :param cur:
    :param log_id: 日志id
    :param page: 处理的页数
    :param request_url: 请求url
    :param request_method: 请求方法
    :param request_params: 请求参数
    :return:
    """
    resp = session.request(method=request_method, url=request_url, data=request_params,headers=HEADERS).text
    rows = parse_table(resp)
    for row in rows:
        row['logId'] = log_id
        insertLogToDB(cur, row, TAGET_TABLE)


def loop_pages(total_pages: int,
               session: Session,
               cur,
               log_id: int,
               request_url: str,
               request_method: str,
               request_params: dict):
    """
    按总页数 循环读取各页数据并处理每一页的数据
    :param cur:
    :param session:
    :param log_id:
    :param request_params:
    :param request_method:
    :param request_url:
    :param total_pages:
    :return:
    """
    for page in range(1, total_pages + 1, 1):
        request_params['pagenum'] = str(page)
        print(page)
        process_one_page(session, cur, log_id, request_url, request_method, request_params)


def get_total_page(table_str):
    """
    从返回的table中获取页数
    :param table_str:
    :return:
    """
    count =json.loads(table_str)['Count']
    return math.ceil(int(count) / 500)


def get_row(props: list, tds: list):
    """
    爬取的数据是一个table表格
    :param props: 列名
    :param tds: table中一行数据（获取table的一个td标签）
    :return:
    """
    row = {}
    for index, prop in enumerate(props):
        row[prop] = tds[index].text.strip()
    return row


def parse_table(table_str):
    """
    解析页面的产品 返回一个产品列表
    :param table_str:
    :return:
    """
    rep = json.loads(table_str)['List']
    rows = []
    index = 0
    for product in rep:
        print(index)
        index = index+1
        if ('cptzxzms' in product):
            cptzxzms = product['cptzxzms']
        else:
            cptzxzms = ''
        cpxsqyName = []
        if not product['cpxsqy'] == '':
            cpxsqyList = product['cpxsqy'].split(',')
            for cpxsqy in cpxsqyList:
                cpxsqyName.append(City[cpxsqy])
            cpxsqy = ','.join(cpxsqyName)
        else:
            cpxsqy = '不限'
        row = {
            'djbm': product['cpdjbm'],
            'fxjg': product['fxjgms'],
            'yzms': product['cpyzmsms'],
            'mjfs': product['mjfsms'],
            'qxlx': product['qxms'],
            'mjbz': product['mjbz'],
            'tzxz': cptzxzms,
            'fxdj': product['fxdjms'],
            'mjqsrq': product['mjqsrq'],
            'mjjsrq': product['mjjsrq'],
            'cpqsrq': product['cpqsrq'],
            'cpjsrq': product['cpyjzzrq'],
            'ywqsrq': product['kfzqqsr'],
            'ywjsrq': product['kfzqjsr'],
            'sjts': product['cpqx'],
            'csjz': product['csjz'],
            'cpjz': product['cpjz'],
            'ljjz': product['ljjz'],
            'zjycdfsyl': product['syl'],
            'cptssx': '',
            'tzzclx': product['tzlxms'],
            'dxjg': '',
            'yqzdsyl': product['yjkhzdnsyl'],
            'yqzgsyl': product['yjkhzgnsyl'],
            'yjbjjzxx': product['yjbjjzxx'],
            'yjbjjzsx': product['yjbjjzsx'],
            'xsqy': cpxsqy
        }
        rows.append(row)
    return rows


if __name__ == '__main__':
    process_flow(log_name=LOG_NAME,
                 target_table=TAGET_TABLE,
                 callback=process_spider_detail
                 )
