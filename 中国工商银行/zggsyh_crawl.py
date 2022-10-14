import requests
from bs4 import BeautifulSoup
from requests import Session
import re

from crawl_utils.global_config import get_table_name
from utils.db_utils import close, get_conn_oracle
from utils.mark_log import mark_failure_log, getLocalDate, insertLogToDB
from utils.spider_flow import process_flow

MASK = 'zggsyh'
TAGET_TABLE = get_table_name(MASK)
LOG_NAME = '中国工商银行'

HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Encoding': 'gzip, deflate,br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Length': '344',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Cookie': 'isP3bank=1; isEn_US=0; isPri=; firstZoneNo=%E6%B9%96%E5%8D%97_1900; BIGipServermybank_10756_pool_shuangzhan_slb=vi2403ffc0131e040bdf4e342e5d1588f6.35887; ariaDefaultTheme=undefined; CK_ISW_EBANKP_EBANKP-WEB-IPV6-NEW_80=bambddmfemcejnia-10|YwbR+|YwbQO',
    'Host': 'mybank.icbc.com.cn',
    'Origin': 'https://mybank.icbc.com.cn',
    'Pragma': 'no-cache',
    'Referer': 'https://mybank.icbc.com.cn/servlet/ICBCBaseReqServletNoSession?dse_operationName=per_FinanceCurProListP3NSOp&p3bank_error_backid=120103&pageFlag=0&Area_code=1900&requestChannel=302',
    'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Microsoft Edge";v="104"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'Sec-Fetch-Dest': 'iframe',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36 Edg/104.0.1293.63'
}

TYPES = [
    {
        'url': 'https://mybank.icbc.com.cn/servlet/ICBCBaseReqServletNoSession',
        'method': 'post',
        'requestParams': {
            'dse_operationName': 'per_FinanceCurProListP3NSOp',
            'financeQueryCondition': '',
            'useFinanceSolrFlag': '1',
            'orderclick': '0',
            'menuLabel': '11|ALL',
            'pageFlag_turn': '2',
            'nowPageNum_turn': '1',
            'Area_code': '1900',
            'structCode': '1'
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
        resp = session.request(method=request_method, url=request_url, data=request_params, headers=HEADERS).text
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
    resp = session.request(method=request_method, url=request_url, data=request_params, headers=HEADERS).text
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
        request_params['nowPageNum_turn'] = str(page)
        print(page)
        process_one_page(session, cur, log_id, request_url, request_method, request_params)


def get_total_page(table_str):
    """
    从返回的table中获取页数
    :param table_str:
    :return:
    """
    soup = BeautifulSoup(table_str, 'lxml')
    totale = soup.select('.ebdp-pc4promote-pageturn')[0]
    totale_ = totale.select('b')[0]
    totale_ = totale_.text
    return int(totale_)


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
    soup = BeautifulSoup(table_str, 'lxml')
    divs = soup.select('.ebdp-pc4promote-circularcontainer-head')
    tables = soup.select('tbody')
    rows = []
    for div, tr in zip(divs, tables):
        IssuingBank = div.select('.ebdp-pc4promote-circularcontainer-tipTitle')
        tzxz = ''
        cpbm = div.select('a[href]')[0].attrs['href']
        cpbm = re.findall("'([^']*)'", cpbm)[0]
        if div.select('.ebdp-pc4promote-circularcontainer-tip-gain'):
            tzxz = '固定收益类'
        if div.select('.ebdp-pc4promote-circularcontainer-tip-mixed'):
            tzxz = '混合类'
        if div.select('.ebdp-pc4promote-circularcontainer-tip-tradition'):
            tzxz = '传统产品'
        # if div.select('.ebdp-pc4promote-circularcontainer-tip-si'):
        #     print('私人')
        if len(IssuingBank) == 0:
            IssuingBank = '自营'
        else:
            IssuingBank = IssuingBank[0].text
        State = div.select('.ebdp-pc4promote-circularcontainer-text1')[0].text
        ProdName = div.select('a')[0].text
        clylnh = ''
        qrnhsyl = ''
        jygynhsyl = ''
        jsgynhsyl = ''
        jlgynhsyl = ''
        yqnhsyl = ''
        yjbjjz = ''
        PurStarAmo = ''
        ProdLimit = ''
        RiskLevel = ''
        SharesGain = ''
        UnitNet = ''
        for index in range(tr.select('.ebdp-pc4promote-doublelabel-content').__len__()):
            title = tr.select('.ebdp-pc4promote-doublelabel-text')[index].text
            if title == '成立以来年化收益':
                clylnh = tr.select('.ebdp-pc4promote-doublelabel-content')[index].text
            elif title == '七日年化收益率':
                qrnhsyl = tr.select('.ebdp-pc4promote-doublelabel-content')[index].text
            elif title == '近一月年化收益':
                jygynhsyl = tr.select('.ebdp-pc4promote-doublelabel-content')[index].text
            elif title == '近三月年化收益':
                jsgynhsyl = tr.select('.ebdp-pc4promote-doublelabel-content')[index].text
            elif title == '近六月年化收益':
                jlgynhsyl = tr.select('.ebdp-pc4promote-doublelabel-content')[index].text
            elif title == '预期年化收益率':
                yqnhsyl = tr.select('.ebdp-pc4promote-doublelabel-content')[index].text
            elif title == '业绩比较基准':
                yjbjjz = tr.select('.ebdp-pc4promote-doublelabel-content')[index].text
            elif title == '起购金额':
                PurStarAmo = tr.select('.ebdp-pc4promote-doublelabel-content')[index].text
            elif title == '期限':
                ProdLimit = tr.select('.ebdp-pc4promote-doublelabel-content')[index].text
            elif title in ('销售风险等级', '产品风险等级'):
                RiskLevel = tr.select('.ebdp-pc4promote-doublelabel-content')[index].text
            elif title == '每万份收益（元）':
                SharesGain = tr.select('.ebdp-pc4promote-doublelabel-content')[index].text
            elif title == '单位净值':
                UnitNet = tr.select('.ebdp-pc4promote-doublelabel-content')[index].text
        row = {
            'fxjg': IssuingBank,
            'cpzt': State,
            'cpbm': cpbm,
            'cpmc': ProdName,
            'tzxz': tzxz,
            'jz': UnitNet,
            'mwfsy': SharesGain,
            'clylnh': clylnh,
            'qrnhsyl': qrnhsyl,
            'jygynhsyl': jygynhsyl,
            'jsgynhsyl': jsgynhsyl,
            'jlgynhsyl': jlgynhsyl,
            'yqnhsyl': yqnhsyl,
            'yjbjjz': yjbjjz,
            'qgje': PurStarAmo,
            'zdcyqx': ProdLimit,
            'fxdj': RiskLevel
        }
        rows.append(row)
    return rows


def do_crawl(config):
    # if __name__ == '__main__':
    process_flow(log_name=LOG_NAME,
                 target_table=TAGET_TABLE,
                 callback=process_spider_detail
                 )
