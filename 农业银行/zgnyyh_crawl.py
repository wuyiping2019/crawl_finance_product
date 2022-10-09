import json
import math
import time

from requests import Session
from browsermobproxy import Server

from utils.db_utils import select_from_db, update_else_insert_to_db
from utils.mark_log import insertLogToDB,getLocalDate
from utils.spider_flow import SpiderFlow, process_flow

TAGET_TABLE = 'ip_bank_abc_personal'
LOG_NAME = '中国农业银行'

HEADERS_PC = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Host": "ewealth.abchina.com",
    "Pragma": "no-cache",
    "Referer": "https//ewealth.abchina.com/fs/filter/default_9148.htm",
    "sec-ch-ua": "Chromium;v=104, Not A;Brand;v=99, Google Chrome;v=104",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Windows",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}

TYPES_PC = [
    {
        'url': 'https://ewealth.abchina.com/app/data/api/DataService/BoeProductV2',
        'method': 'get',
        'requestParams': {
            "i": "1",
            "s": "15",
            "o": "0",
            "w": "%7C%7C%7C%7C%7C%7C%7C1%7C%7C0%7C%7C0"
        }
    }
]


HEADERS_MOBILE = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Length': '128',
    'Content-Type': 'application/json;charset=UTF-8',
    'Host': 'wx.abchina.com',
    'Origin': 'https://wx.abchina.com',
    'Pragma': 'no-cache',
    'Referer': 'https://wx.abchina.com/webank/main-view/financial',
    'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Mobile Safari/537.36'
}

TYPES_MOBILE = [
    {
        'url': 'https://wx.abchina.com/webank/main/financing/qryFinancingList',
        'method': 'post',
        'requestParams': {
            'financingType': "01",
            'pageCurrCount': "0",
            'productType': "",
            'profitType': "",
            'qryType': "01",
            'sortKey': "",
            'sortType': "",
            'vluTerm': ""
        }
    }
]


HEADERS_MOBILE_DETAIL = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Content-Length": "24",
    "Content-Type": "application/json;charset=UTF-8",
    "Cookie": "BIGipServerpool_wx.abchina.com_443_webank_main-view=!9uBIYsmbIbUvSilp/jyGVyUiAHC9BzIHRoWtKT/SfrFBQxRoJRbWNZX5CMcskmyvHvpm0EO9epeU; BIGipServerpool_wx.abchina.com_443_webank_main=!qd9oOPf+uLOkOj5p/jyGVyUiAHC9ByVt4pjeIZ68TzXVlfwBmWbpcD8CW4qkEEK232zymBnOXR09; WT_FPC=id=2ac3a2b7eee5bd5757f1660717902491lv=1662364068180ss=1662362023562",
    "Host": "wx.abchina.com",
    "Origin": "https//wx.abchina.com",
    "Pragma": "no-cache",
    "Referer": "https//wx.abchina.com/webank/main-view/ProductDetails?productId=AD141862&besellout=1",
    "sec-ch-ua": "\"Chromium\";v=\"104\", \" Not A;Brand\";v=\"99\", \"Google Chrome\";v=\"104\"",
    "sec-ch-ua-mobile": "?1",
    "sec-ch-ua-platform": "\"Android\"",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Mobile Safari/537.36"
}

MOBILE_DETAIL_URL = 'https://wx.abchina.com/webank/main/financing/qryFinancingDetail'
MOBILE_DETAIL_METHOD = 'post'


# PC端
def process_pc(conn, cursor, session: Session, log_id: int, **kwargs):
    """
    处理爬虫细节的回调函数
    :param conn:自动传参
    :param cursor: 自动传参
    :param session: 自动传参
    :param log_id: 自动传参
    :param kwargs: 额外需要的参数 手动传入
    :return:
    """
    for type in TYPES_PC:
        request_url = type['url']
        request_method = type['method']
        request_params = type['requestParams']
        resp = session.request(method=request_method, url=request_url, params=request_params, headers=HEADERS_PC).text
        # time.sleep(3)
        # 获取总页数
        rep = json.loads(resp)['Data']
        table1 = rep['Table1']
        total = table1[0]
        total_ = total['total']
        page = math.ceil(int(total_) / 15)
        # total_page = get_total_page(resp)
        # 循环处理每页数据
        # loop_pages(total_, session, cursor, log_id, request_url, request_method, request_params)
        for page in range(1, page + 1, 1):
            request_params['i'] = str(page)
            print(page)
            # process_one_page(session, cursor, log_id, request_url, request_method, request_params)
            resp = session.request(method=request_method, url=request_url, params=request_params,headers=HEADERS_PC).text
            # time.sleep(3)
            # rows = parse_table(resp)
            if resp and not 'respCode' in resp:
                rep = json.loads(resp)['Data']
                tables = rep['Table']
                rows = []
                for product in tables:
                    row = {
                        'collectionMethod': product['collectionMethod'],
                        'IsCanBuy': product['IsCanBuy'],
                        'yzms': product['ProdClass'],
                        'sylx': product['ProdYildType'],
                        'zdcyqx': product['ProdLimit'],
                        'yjbjjz': product['ProdProfit'],
                        'qgje': product['PurStarAmo'],
                        'xsqy': product['ProdArea'],
                        'fsqsrq': product['ProdSaleDate'][0:8],
                        'fsjzrq': product['ProdSaleDate'][9:],
                        'PrdYildTypeOrder': product['PrdYildTypeOrder'],
                        'cpbm': product['ProductNo'],
                        'cpmc': product['ProdName'],
                        'fxjg': product['issuingOffice'],
                        'yjjzIntro': product['yjjzIntro'],
                        'szComDat': product['szComDat']
                    }
                    rows.append(row)
            if not rows == None:
                for row in rows:
                    row['logId'] = log_id
                    insertLogToDB(cursor, row, TAGET_TABLE)
        break





# 移动端
def process_mobile(conn, cursor, session: Session, log_id: int, **kwargs):
    """
    处理爬虫细节的回调函数
    :param conn:自动传参
    :param cursor: 自动传参
    :param session: 自动传参
    :param log_id: 自动传参
    :param kwargs: 额外需要的参数 手动传入
    :return:
    """
    for type in TYPES_MOBILE:
        request_url = type['url']
        request_method = type['method']
        request_params = type['requestParams']
        resp = session.request(method=request_method, url=request_url, data=json.dumps(request_params), headers=HEADERS_MOBILE).text
        time.sleep(3)
        # 获取总页数
        rep = json.loads(resp)['result']
        page = rep['totalCount']
        # total_page = get_total_page(resp)
        # 循环处理每页数据
        # loop_pages(total_, session, cursor, log_id, request_url, request_method, request_params)
        for page in range(page):
            request_params['pageCurrCount'] = str(page)
            print(page)
            # process_one_page(session, cursor, log_id, request_url, request_method, request_params)
            resp = session.request(method=request_method, url=request_url, data=json.dumps(request_params),headers=HEADERS_MOBILE).text
            time.sleep(3)
            # rows = parse_table(resp)
            res = json.loads(resp)
            if resp and res['status'] == '0000':
                result = res['result']
                productList = result['productList']
                rows = []
                for product in productList:
                    productNo = product['productNo']
                    resp = session.request(method=MOBILE_DETAIL_METHOD, url=MOBILE_DETAIL_URL, data=json.dumps({'productId':productNo}),headers=HEADERS_MOBILE_DETAIL).text
                    res = json.loads(resp)
                    if resp and res['status'] == '0000':
                        result = res['result']
                        row = {
                            'cpbm':productNo,
                            'dzje': result['amtTrStep'],
                            'qgje': result['amtTrStrt'],
                            'mjfs': result['codClctMod'],
                            'fsjzrq': result['dateClctEnd'][0:10],
                            'fsqsrq': result['dateClctStrt'][0:10],
                            'dqr': result['dateMatu'][0:10],
                            'qxr': result['dateVluStrt'][0:10],
                            'sylx': result['ifPdPftTyp'],
                            'mjbz': result['isoCcy'],
                            'tzxz': result['pdInvsDrec'],
                            'cpmc': result['productName'],
                            'xsqy': result['saleBrchList'],
                            'fxdj': result['tarfRskLvl'],
                            'yjbjjz': result['txtEstPftRate'],
                            'tzqx': result['txtIfTarfTerm'],
                            'zxrgdw': result['vluPdPrce']
                        }
                        rows.append(row)
            if not rows == None:
                for row in rows:
                    row['logId'] = log_id
                    row['createTime'] = getLocalDate()
                    update_else_insert_to_db(cursor, TAGET_TABLE, row,{'logid':log_id,'cpbm':row['cpbm']})
        break


class SpiderFlowImpl(SpiderFlow):
    def callback(self, conn, cursor, session: Session, log_id: int, **kwargs):
        proxy = None
        f = open('crawl.txt', 'w')
        try:
            process_pc(conn,cursor, session, log_id,**kwargs)
            process_mobile(conn,cursor, session, log_id,**kwargs)
            # process(cursor, session, 874, proxy)
        except Exception as e:
            raise e
        finally:
            if proxy:
                proxy.close()
            if f:
                f.close()



if __name__ == '__main__':
    # process_flow(log_name=LOG_NAME,target_table=TAGET_TABLE,callback=process_spider_detail)
    process_flow(LOG_NAME, TAGET_TABLE, SpiderFlowImpl())
