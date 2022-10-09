import requests
import json
from bs4 import BeautifulSoup
from requests import Session

from utils.db_utils import close, get_conn_oracle
from utils.mark_log import mark_failure_log, getLocalDate, insertLogToDB
from utils.spider_flow import process_flow

TAGET_TABLE = 'ip_bank_bc_personal'
LOG_NAME = '中国银行'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)',
    'Refer': 'https://www.boc.cn/fimarkets/cs8/',
    'Accept': 'text/html, application/xhtml+xml, image/jxr, */*'
}

TYPES = [
    {
        'url': 'https://www.boc.cn/fimarkets/cs8/201109/t20110922_1532694.html',
        'method': 'get',
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
        request_url = type['url']
        request_method = type['method']
        process_one_page(session, cursor, log_id, request_url, request_method)
        break


def process_one_page(session: Session,
                     cur,
                     log_id: int,
                     request_url: str,
                     request_method: str):
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
    resp = session.request(method=request_method, url=request_url, headers=HEADERS).content.decode(encoding='utf-8')
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
        row[prop] = tds[index].strip()
    return row


def parse_table(table_str):
    """
    解析页面的产品 返回一个产品列表
    :param type:
    :param table_str:
    :return:
    """
    soup = BeautifulSoup(table_str, 'lxml')
    h1s = soup.select('h1')
    tables = soup.select('table')
    rows = []
    for h1, table in zip(h1s[1:], tables):
        trs = table.select('tr')
        ths = table.select('th')
        ths = [''.join(th.text.split()) for th in ths]
        line_map = {}
        for tr in trs[1:]:
            tds = tr.select('td')
            current_line_index = 0
            for td in tds:
                # 获取当前单元格所占的列数
                colspan = td.get_attribute_list('colspan')[0]
                if not colspan:
                    colspan = 1
                td = ' '.join(td.text.split())
                line_map[current_line_index] = td
                current_line_index += 1
                if not colspan == 1:
                    for i in range(1, int(colspan)):
                        line_map[current_line_index] = td
                        current_line_index += 1
            sale_type = []
            for k, v in line_map.items():
                k = ths[k]
                if k in ['柜面', '网银', '手机银行', '自助终端', '快信通', '微信']:
                    if v == '√':
                        sale_type.append(k)
                    continue
            del line_map[5]
            del line_map[6]
            del line_map[7]
            del line_map[8]
            del line_map[9]
            del line_map[10]
            sale_type = ','.join(sale_type)
            line_map[5] = sale_type
            if h1.text in ('净值型产品(中银理财子公司旗下)','人民币理财产品','外币理财产品'):
                # line_map[current_line_index] = h1.text
                row = {
                    'cpbm': line_map[0],
                    'cpmc': line_map[1],
                    'cpqx': line_map[2],
                    'yjbjjz_jz': line_map[3],
                    'qgje': line_map[4],
                    'xsqd': line_map[5],
                    'fxdj': line_map[11],
                    'mjqsrq': line_map[12],
                    'mjjsrq': line_map[13],
                    'qxr': line_map[14],
                    'fbq': line_map[15],
                    'tzzlx': line_map[16],
                    'xsqy': line_map[17],
                    'cplx': h1.text
                }
                # row = get_row(
                #     ['ProductCode', 'ProductName', 'ProdLimit', 'ProdProfit', 'PurStarAmo', 'Countertop',
                #      'InternetBank', 'MobileBank', 'SelfTerminal',
                #      'FastCommunication', 'WeChat', 'RiskLevel', 'CollectionStartDate', 'CollectionEndDate',
                #      'InterestCommencementDate', 'MaturityDate',
                #      'InvestorsType', 'ProdArea','ProductType'], line_map)
            elif h1.text == '长期开放类理财产品':
                # line_map[current_line_index] = h1.text
                row = {
                    'cpbm': line_map[0],
                    'cpmc': line_map[1],
                    'cpqx': line_map[2],
                    'yjbjjz_jz': line_map[3],
                    'qgje': line_map[4],
                    'xsqd': line_map[5],
                    'fxdj': line_map[11],
                    'sggzsm': line_map[12],
                    'hsgzsm': line_map[13],
                    'tzzlx': line_map[14],
                    'xsqy': line_map[15],
                    'cplx': h1.text
                }

                # row = get_row(
                #     ['ProductCode', 'ProductName', 'ProdLimit', 'ProdProfit', 'PurStarAmo', 'Countertop',
                #      'InternetBank', 'MobileBank', 'SelfTerminal',
                #      'FastCommunication', 'WeChat', 'RiskLevel', 'PurchaseInstructions', 'RedemptionInstructions',
                #      'InvestorsType', 'ProdArea','ProductType'], line_map)
            rows.append(row)
    return rows


if __name__ == '__main__':
    process_flow(log_name=LOG_NAME,
                 target_table=TAGET_TABLE,
                 callback=process_spider_detail
                 )
