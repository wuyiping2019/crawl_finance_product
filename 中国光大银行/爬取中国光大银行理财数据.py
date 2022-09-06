import requests
from bs4 import BeautifulSoup
from requests import Session

from utils.db_utils import close, get_conn_oracle
from utils.mark_log import mark_failure_log, getLocalDate, insertLogToDB
from utils.spider_flow import process_flow

TAGET_TABLE = 'ip_bank_ceb_personal'
LOG_NAME = '中国广大银行'

HEADERS = {
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Host': 'www.cebbank.com',
    'Origin': 'http://www.cebbank.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

TYPES = [
    {
        'type': '理财产品（自营）',
        'url': 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/qqdFrontAction!listProduct.action',
        'method': 'post',
        'requestParams': {
            'proName': '',
            'bankProCode': '',
            'valid': '',
            'pfcatCode': 'SNOTE',
            'page': '1',
            'self_support': '0'
        }

    },
    {
        'type': '理财产品（代销商业银行理财子公司产品',
        'url': 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/qqdFrontAction!listProduct.action',
        'method': 'post',
        'requestParams': {
            'proName': '',
            'bankProCode': '',
            'valid': '',
            'pfcatCode': 'SNOTE',
            'page': '1',
            'self_support': '1'
        }
    },
    {
        'type': '基金（代销）',
        'url': 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/qqdFrontAction!listProduct.action',
        'method': 'post',
        # 表单数据
        'requestParams': {
            'proName': '',
            'bankProCode': '',
            'valid': '',
            'pfcatCode': 'FUND',
            'page': '1',
            'self_support': ''
        }
    },
    {
        'type': '集合资产管理计划（代销）',
        'url': 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/qqdFrontAction!listProduct.action',
        'method': 'post',
        # 表单数据
        'requestParams': {
            'proName': '',
            'bankProCode': '',
            'valid': '',
            'pfcatCode': 'BROKE',
            'page': '1',
            'self_support': ''
        }
    },
    {
        'type': '保险（代销）',
        'url': 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/qqdFrontAction!listProduct.action',
        'method': 'post',
        # 表单数据
        'requestParams': {
            'proName': '',
            'bankProCode': '',
            'valid': '',
            'pfcatCode': 'INS',
            'page': '1',
            'self_support': ''
        }
    },
    {
        'type': '信托（代销）',
        'url': 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/qqdFrontAction!listProduct.action',
        'method': 'post',
        # 表单数据
        'requestParams': {
            'proName': '',
            'bankProCode': '',
            'valid': '',
            'pfcatCode': 'TRUST',
            'page': '1',
            'self_support': ''
        }
    },
    {
        'type': '国债（代销）',
        'url': 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/qqdFrontAction!listProduct.action',
        'method': 'post',
        # 表单数据
        'requestParams': {
            'proName': '',
            'bankProCode': '',
            'valid': '',
            'pfcatCode': 'BOND',
            'page': '1',
            'self_support': ''
        }
    },
    {
        'type': '贵金属（自营）',
        'url': 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/qqdFrontAction!listProduct.action',
        'method': 'post',
        # 表单数据
        'requestParams': {
            'proName': '',
            'bankProCode': '',
            'valid': '',
            'pfcatCode': 'GOLDFX',
            'page': '1',
            'self_support': ''
        }
    },
    {
        'type': '贵金属（代销）',
        'url': 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/qqdFrontAction!listProduct.action',
        'method': 'post',
        # 表单数据
        'requestParams': {
            'proName': '',
            'bankProCode': '',
            'valid': '',
            'pfcatCode': 'GOLDFX',
            'page': '1',
            'self_support': '1'
        }
    },
    {
        'type': '开放银行（代销）',
        'url': 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/qqdFrontAction!showType.action&pfcatCode=SBANK',
        'method': 'get',
        # 表单数据
        'requestParams': {
        }
    },
    {
        'type': '存款（自营）',
        'url': 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/qqdFrontAction!listProduct.action',
        'method': 'post',
        'requestParams': {
            'proName': '',
            'bankProCode': '',
            'valid': '',
            'pfcatCode': 'SAVING',
            'page': '1',
            'self_support': ''
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
    for type in [TYPES[0]]:
        type_name = type['type']  # 爬取的数据中含有
        request_url = type['url']
        request_method = type['method']
        request_params = type['requestParams']
        resp = session.request(method=request_method, url=request_url, data=request_params, headers=HEADERS).text
        # 获取总页数
        total_page = get_total_page(resp)
        # 循环处理每页数据
        loop_pages(total_page, session, cursor, log_id, request_url, request_method, request_params, type_name)
        break


def process_one_page(session: Session,
                     cur,
                     log_id: int,
                     request_url: str,
                     request_method: str,
                     request_params: dict,
                     type: str):
    """
    处理爬取的一页数据并写入数据库
    :param type:
    :param session:
    :param cur:
    :param log_id: 日志id
    :param page: 处理的页数
    :param request_url: 请求url
    :param request_method: 请求方法
    :param request_params: 请求参数
    :return:
    """
    resp = session.request(method=request_method, url=request_url, params=request_params, headers=HEADERS).text
    rows = parse_table(resp, type)
    for row in rows:
        row['logId'] = log_id
        insertLogToDB(cur, row, TAGET_TABLE)


def loop_pages(total_pages: int,
               session: Session,
               cur,
               log_id: int,
               request_url: str,
               request_method: str,
               request_params: dict,
               type: str):
    """
    按总页数 循环读取各页数据并处理每一页的数据
    :param type:
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
        request_params['page'] = str(page)
        process_one_page(session, cur, log_id, request_url, request_method, request_params, type)


def get_total_page(table_str):
    """
    从返回的table中获取页数
    :param table_str:
    :return:
    """
    soup = BeautifulSoup(table_str, 'lxml')
    return int(soup.select('#totalPage')[0].get('value').strip())


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


def parse_table(table_str, type: str):
    """
    解析页面的产品 返回一个产品列表
    :param type:
    :param table_str:
    :return:
    """
    soup = BeautifulSoup(table_str, 'lxml')
    trs = soup.select('tr')
    rows = []
    for tr in trs[1:]:
        tds = tr.select('td')
        row = None
        if type in ['理财产品（自营）', '理财产品（代销商业银行理财子公司产品）']:
            row = get_row(
                ['type', 'prdName', 'prdCode', 'prdProp', 'risk', 'detailType', 'issuer', 'onSale', 'prdDuration',
                 'openRule', 'minAmount'], tds)
        elif type == '基金（代销）':
            row = get_row(
                ['type', 'prdName', 'prdCode', 'prdProp', 'risk', 'detailType',
                 'saleRegion', 'issuer', 'onSale',
                 'openCloseDuration', 'prdDuration', 'minAmount'], tds)
        elif type == '集合资产管理计划（代销）':
            row = get_row(
                ['type', 'prdName', 'prdCode', 'prdProp', 'risk', 'saleRegion', 'issuer', 'onSale', 'openCloseDuration',
                 'prdDuration', 'minAmount'], tds)
        elif type == '保险（代销）':
            row = get_row(['type', 'prdName', 'prdCode', 'risk', 'saleRegion', 'issuer', 'onSale'], tds)
        elif type == '信托（代销）':
            row = get_row(
                ['type', 'prdName', 'prdCode', 'prdProp', 'risk', 'saleRegion', 'issuer', 'onSale', 'openCloseDuration',
                 'prdDuration', 'minAmount'], tds)
        elif type == ['国债（代销）', '存款（自营）']:
            row = get_row(['type', 'prdName', 'prdCode', 'risk', 'saleRegion', 'issuer', 'onSale', 'openCloseDuration',
                           'prdDuration', 'minAmount'], tds)
        elif type == '贵金属（自营）':
            row = get_row(['type', 'prdName', 'prdCode', 'risk', 'saleRegion',
                           'issuer', 'onSale', 'openCloseDuration', 'minAmount'], tds)
        elif type == '开放银行（代销）':
            row = get_row(['prdName', 'risk', 'saleRegion', 'issuer', 'onSale'], tds)
            row['type'] = type
        elif type == '开放银行（代销）':
            row = get_row(['type', 'prdName', 'prdCode', 'risk', 'saleRegion', 'issuer', 'onSale', 'openCloseDuration',
                           'prdDuration', 'minAmount'], tds)
        rows.append(row)
    return rows


if __name__ == '__main__':
    process_flow(log_name=LOG_NAME,
                 target_table=TAGET_TABLE,
                 callback=process_spider_detail
                 )
