import json
import math
from zgmsyh_config import FIELD_MAPPINGS


def get_pages(driver):
    result = driver.execute_script('return window.getData(1)')
    loads = json.loads(result)
    return math.ceil(loads['totalSize'] / loads['pageSize'])


def get_target_page(page_no, driver):
    """
    获取的是一页数据的json字符串
    :param page_no:
    :param driver:
    :return:
    """
    return driver.execute_script(f'return window.getData({page_no})')


def parse_page_json(page_json):
    loads = json.loads(page_json)
    prd_list = loads['prdList']
    income_infos = loads['incomeInfos']
    rows = []
    for prd_info, income_info in zip(prd_list, income_infos):
        row = {
            FIELD_MAPPINGS['产品名称']: prd_info['prdName'],
            FIELD_MAPPINGS['产品简称']: prd_info['prdShortName'],
            FIELD_MAPPINGS['产品编码']: prd_info['prdCode'],
            # 'cply': '中国民生银行',  # 产品来源
            FIELD_MAPPINGS['币种']: prd_info['currTypeName'],
            FIELD_MAPPINGS['起购金额']: str(prd_info['pfirstAmt']) + '元',
            FIELD_MAPPINGS['风险等级']: prd_info['riskLevelName'],
            FIELD_MAPPINGS['募集起始日期']: prd_info['ipoStartDate'],
            FIELD_MAPPINGS['募集结束日期']: prd_info['ipoEndDate'],
            FIELD_MAPPINGS['产品额度']: prd_info['totAmt'],
            FIELD_MAPPINGS['剩余额度']: prd_info['usableAmt'],
            FIELD_MAPPINGS['产品起始日期']: prd_info['startDate'],
            FIELD_MAPPINGS['产品结束日期']: prd_info['endDate'],
            FIELD_MAPPINGS['下一个开放日']: prd_info['nextOpenDate'][0],
            FIELD_MAPPINGS['运作模式']: prd_info['prdTypeName'],
            FIELD_MAPPINGS['业绩比较基准']: ':'.join(prd_info['divModesName'].split(':')[::-1]) if len(
                prd_info['divModesName'].split(':')) == 2
            FIELD_MAPPINGS['净值']: prd_info.get('NAV', None),
            FIELD_MAPPINGS['净值日期']: prd_info.get('navDate', None),
            FIELD_MAPPINGS['管理人']: prd_info.get('prdManagerName', None),
        }
        rows.append(row)
    return rows


def exec_crawl_pc(session):
    url = 'http://www.cmbc.com.cn/gw/po_web/QryProdListOnMarket.do'
    data = {
        "currTypeList": [],
        "keyWord": "",
        "currentIndex": 0,
        "fundModeList": [],
        "orderFlag": "1",
        "pageNo": 1,
        "pageSize": 10,
        "pfirstAmtList": [],
        "prdChara": "4",
        "prdTypeNameList": [],
        "$FF_HEADER$": {
            "appId": "",
            "appVersion": "",
            "device": {
                "osType": "BROWSER",
                "osVersion": "",
                "uuid": ""
            }
        }
    }
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
               "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Content-Length": "261",
               "Content-Type": "application/json;charset=UTF-8",
        "Host": "www.cmbc.com.cn",
               "Origin": "http//www.cmbc.com.cn",
        "Pragma": "no-cache",
        "Proxy-Connection": "keep-alive",
               "Referer": "http//www.cmbc.com.cn/xmhpd/grkh/rmcp/rmlc/index.htm",
               "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
               "X-Requested-With": "XMLHttpRequest"
    }
