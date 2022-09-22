import json
from typing import List

import requests
from dbutils.pooled_db import PooledDB

from utils.crawl_request_utils import CrawlRequest
from utils.db_utils import get_db_poll
from utils.global_config import get_table_name, get_sequence_name, get_trigger_name, DB_ENV, DBType

__url = 'https://wap.cgbchina.com.cn/h5-mobilebank-app/noSessionServlet/hbss/fn20027.lgx'
__headers = {
    'host': 'wap.cgbchina.com.cn',
    # 'content-length': 424,
    'accept': 'application/json, text/plain, */*',
    'origin': 'https://wap.cgbchina.com.cn',
    'sendersn': '1663749151600n2005493',
    'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x6700143B) WindowsWechat(0x6307062c)',
    'content-type': 'application/json;charset=UTF-8',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'cors',
    'sec-fetch-dest': 'empty',
    'referer': 'https://wap.cgbchina.com.cn/h5-mobilebank-web/h5/investment/self/list?srcChannel=WX&secondaryChannel=WX&mainChannel=400&tab=1&srcScene=GFYHGZH&channel=400&sChannel=MB&faceFlag=LS&isRegistCS=1&HMBA_STACK_HASH=1663748433050',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
}
__method = 'POST'
__data = lambda page_no: {
    "body": {
        "beginNum": (page_no - 1) * 20,
        "fetchNum": 20,
        "channel": "400",
        "sChannel": "WX",
        "structDepPrdFlag": "N",
        "tagflagNew": None,
        "prdCycle": "",
        "firstAmt": "",
        "sortFlag": "0",
        "curType": "",
        "riskLevel": "",
        "prdManagerList": [],
        "expireType": "",
        "prdSellStatus": ""
    },
    "header": {
        "senderSN": "1663749151600n2005493",
        "os": "Win32",
        "channel": "WX",
        "secondChannel": "",
        "scope": "2",
        "mpSId": "HMBS_C882C49E556385209F40A14EC9972733_1551629178531586048",
        "utmSource": ""
    }
}


def __end_func(crawl_request: CrawlRequest, response):
    resp_str = response.text \
        .encode(response.encoding) \
        .decode('utf-8', errors="ignore") \
        if response.encoding \
        else response.text
    loads = json.loads(resp_str)['body']['list']
    if not loads:
        crawl_request.end_flag = True


def __switch_page_no(crawl_request: CrawlRequest):
    page_no = crawl_request.page_no
    crawl_request.request['json'] = __data(page_no)


def __to_rows(response):
    resp_str = response.text \
        .encode(response.encoding) \
        .decode('utf-8', errors="ignore") \
        if response.encoding \
        else response.text
    loads = json.loads(resp_str)['body']['list']
    return loads


__field_mapping_funcs = {
    'totLimitStr': lambda x: str(x) + '元' if not str(x).endswith('元') else str(x),
    'issPrice': lambda x: str(x) + '元' if not str(x).endswith('元') else str(x),
}
__mapping_dicts = {

}
__transformed_keys = {
    'prdName': '产品名称',
    'prdName2': '产品简称',
    'prdCode': '产品编码',
    'tACode': 'TA编码',
    'tAName': 'TA名称',
    'riskLevel': '风险等级',
    'investManagerName': '管理人',
    'issPrice': '发行价',
    'iPOEndDate': '募集结束日期',
    'iPOStartDate': '募集起始日期',
    'estabDate': '产品起始日期',
    'endDate': '产品结束日期',
    'curType': '币种',
    'nAV': '净值',
    'nAVDate': '净值日期',
    'totLimitStr': '总额度',
    'yieldName': 'yieldName',
    'yieldName2': 'yieldName2',
    'yieldVal2': 'yieldVal2'

}
__temp_transformed_key = ['yieldName', 'yieldName2', 'yieldVal2']


def __row_post_processor_func(self: CrawlRequest, row: dict):
    if {'yieldName', 'yieldName2', 'yieldVal2'}.issubset(row.keys()):
        if row['yieldName'] != '单位净值':
            row['yjbjjz'] = {
                'title': row['yieldName'],
                'value': row['yieldVal2']
            }
        else:
            row['yjbjjz'] = {
                'title': '预期收益率',
                'value': row['yieldVal2']
            }

    def __detail_to_rows(response):
        resp_str = response.text \
            .encode(response.encoding) \
            .decode('utf-8', errors='ignore') \
            if response.encoding \
            else response.text
        loads = json.loads(resp_str)
        return [loads['body']]

    __detail_field_mapping_funcs = {'prdAttr': lambda x: '非保本浮动收益类' if str(x) == "1" else '保本浮动收益类'}
    __detail_field_mapping_funcs.update(__field_mapping_funcs)
    detail_crawl_request = CrawlRequest(
        method='post',
        url='https://wap.cgbchina.com.cn/h5-mobilebank-app/noSessionServlet/hbss/fn10025.lgx',
        json={
            "body": {
                "channel": "400", "prdCode": f"{row['cpbm']}", "sChannel": "WX"
            },
            "header": {
                "senderSN": "1663823532558n1005981", "os": "Win32", "channel": "WX", "secondChannel": "",
                "scope": "2", "mpSId": "HMBS_C882C49E556385209F40A14EC9972733_1551629178531586048",
                "utmSource": ""
            }
        },
        headers={
            "host": "wap.cgbchina.com.cn",
            "accept": "application/json, text/plain, */*",
            "origin": "https//wap.cgbchina.com.cn",
            "sendersn": "1663823532558n1005981",
            "user-agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x6700143B) WindowsWechat(0x6307062c)",
            "content-type": "application/json;charset=UTF-8",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "referer": "https//wap.cgbchina.com.cn/h5-mobilebank-web/h5/investment/self/detail_repeat_1663817605945?srcChannel=WX&HMBA_STACK_HASH=1663748433050&prdCode=XFTLCY0601ZSA&mainChannel=400&secondaryChannel=WX&ecifNum=&sourceActivityId=&cusmanagerId=&srcEcifNum=&accOpenNode=&channel=400&sChannel=MB&isRegistCS=1&faceFlag=LS&trade_BranchNo=&loginType=app&signType=&guideBanner=",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7"
        },
        field_mapping_funcs=__detail_field_mapping_funcs,
        mapping_dicts=__mapping_dicts,
        transformed_keys={
            'prdName': '产品名称',
            'prdCode': '产品编码',
            'prdAttr': '收益类型',
            'curType': '币种',
            'prdManagerCode': '管理人编码',
            'prdManagerName': '管理人名称',
            'productContent01': '投资期限',
            'purchaseRule': '购买规则',
            'rateType': '收益规则',
            'redeemType': '赎回规则'
        },
        missing_code_sample=1,
        target_table=get_table_name('gfyh'),
        sequence_name=get_sequence_name('gryh'),
        trigger_name=get_trigger_name('gfyh'),
        db_type=DB_ENV,
        table_str_type='clob' if DB_ENV.value == DBType.oracle.value else 'text' if DBType.value == DBType.mysql.value else None,
        table_number_type='number(11)' if DB_ENV.value == DBType.oracle.value else 'long' if DBType.value == DBType.mysql.value else None,
        log_id=None,
        session=self.session,
        to_rows=__detail_to_rows,
        total_page=1,
        sleep_second=1,
        connection=None,
        end_func=None,
        row_post_processor_func=None,
        temp_transformed_keys=[],
        post_requests=[]
    )
    detail_crawl_request.do_crawl()
    detail_rows = detail_crawl_request.processed_rows
    row.update(detail_rows[0])


def get_code_definition(session=None):
    # 将获取的编码-解码映射 刷新到__mapping_dicts中
    close_flag = False
    if session is None:
        session = requests.session()
        close_flag = True
    method = 'post'
    url = 'https://wap.cgbchina.com.cn/h5-mobilebank-app/noSessionServlet/hbss/fn10026.lgx'
    data = {
        "body": {
            "channel": "400", "sChannel": "WX", "prdType": "1"
        },
        "header": {
            "senderSN": "1663749151603n1009431", "os": "Win32", "channel": "WX", "secondChannel": "",
            "scope": "2", "mpSId": "HMBS_C882C49E556385209F40A14EC9972733_1551629178531586048",
            "utmSource": ""
        }
    }
    headers = {
        "host": "wap.cgbchina.com.cn", "accept": "application/json, text/plain, */*",
        "origin": "https//wap.cgbchina.com.cn", "sendersn": "1663749151603n1009431",
        "user-agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x6700143B) WindowsWechat(0x6307062c)",
        "content-type": "application/json;charset=UTF-8", "sec-fetch-site": "same-origin", "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "referer": "https//wap.cgbchina.com.cn/h5-mobilebank-web/h5/investment/self/list?srcChannel=WX&secondaryChannel=WX&mainChannel=400&tab=1&srcScene=GFYHGZH&channel=400&sChannel=MB&faceFlag=LS&isRegistCS=1&HMBA_STACK_HASH=1663748433050",
        "accept-encoding": "gzip, deflate, br", "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7"
    }
    response = session.request(method=method, url=url, json=data, headers=headers)
    resp_str = response.text \
        .encode(response.encoding) \
        .decode('utf-8', errors='ignore') \
        if response.encoding \
        else response.text
    loads = json.loads(resp_str)
    body = loads['body']
    for k, v in body.items():
        # k - str
        # v - list
        if k not in __mapping_dicts.keys():
            k = k[:-4]
            __mapping_dicts[k] = {}
        for item in v:
            # item - dict
            if k == 'curType':
                __mapping_dicts[k][item['curType']] = item['curTypeName']
            elif k == 'expireType':
                __mapping_dicts[k][item['expireType']] = item['expireTypeName']
            elif k == 'firstAmt':
                __mapping_dicts[k][item['firstAmt']] = item['firstAmtName']
            elif k == 'investManager':
                __mapping_dicts[k][item['investManagerNo']] = item['investManagerName']
            elif k == 'prdLimit':
                __mapping_dicts[k][item['limitTime']] = item['limitTimeName']
            elif k == 'prdSellStatus':
                __mapping_dicts[k][item['prdSellStatus']] = item['prdSellStatusName']
            elif k == 'riskLevel':
                __mapping_dicts[k][item['riskLevel']] = item['riskLevelName']
    if close_flag:
        session.close()


def do_crawl(session, db_poll, log_id) -> CrawlRequest:
    get_code_definition(session)
    crawl = CrawlRequest(
        method=__method,
        url=__url,
        switch_page_func=__switch_page_no,
        json=__data(1),
        headers=__headers,
        field_mapping_funcs=__field_mapping_funcs,
        mapping_dicts=__mapping_dicts,
        transformed_keys=__transformed_keys,
        sequence_name=get_sequence_name('gfyh'),
        trigger_name=get_trigger_name('gfyh'),
        target_table=get_table_name('gfyh'),
        session=session,
        to_rows=__to_rows,
        log_id=log_id,
        end_func=__end_func,
        db_poll=db_poll,
        total_page=1,
        row_post_processor_func=__row_post_processor_func,
        temp_transformed_keys=__temp_transformed_key
    )
    while not crawl.end_flag:
        crawl.do_crawl()
    return crawl


if __name__ == '__main__':
    session = requests.session()
    poll: PooledDB = get_db_poll()
    crawl = None
    try:
        crawl = do_crawl(session=session, db_poll=poll, log_id=-1)
        crawl.do_save()
    except Exception as e:
        print(e)
    finally:
        poll.close()
        session.close()