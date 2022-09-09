import json
import math
import re
import time

import requests

from utils.custom_exception import cast_exception, CustomException
from zgmsyh_config import FIELD_MAPPINGS, SLEEP_SECOND, PATTERN_Z, PATTERN_E


def process_zgmsyh_pc(session):
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
    response = session.post(url=url, json=data, headers=headers)
    time.sleep(SLEEP_SECOND)
    resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
    rows = []
    try:
        loads = json.loads(resp_str)
        total_size = loads['totalSize']
        page_num = math.ceil(total_size / data['pageSize'])
        for page in range(1, page_num + 1):
            data['pageNo'] = page
            resp = session.post(url=url, json=data, headers=headers)
            time.sleep(SLEEP_SECOND)
            resp_str = resp.text.encode(resp.encoding).decode('utf-8') if resp.encoding else resp.text
            page_resp_dict = json.loads(resp_str)
            prd_list = page_resp_dict['prdList']
            for prd_info in prd_list:
                row = {
                    FIELD_MAPPINGS['产品名称']: prd_info.get('prdName', None),
                    FIELD_MAPPINGS['产品简称']: prd_info.get('prdShortName', None),
                    FIELD_MAPPINGS['产品编码']: prd_info.get('prdCode', None),
                    # 'cply': '中国民生银行',  # 产品来源
                    FIELD_MAPPINGS['币种']: prd_info.get('currTypeName', None),
                    FIELD_MAPPINGS['起购金额']: str(prd_info.get('pfirstAmt', None)) + '元',
                    FIELD_MAPPINGS['风险等级']: prd_info.get('riskLevelName', None),
                    FIELD_MAPPINGS['募集起始日期']: prd_info.get('ipoStartDate', None),
                    FIELD_MAPPINGS['募集结束日期']: prd_info.get('ipoEndDate', None),
                    FIELD_MAPPINGS['产品额度']: prd_info.get('totAmt', None),
                    FIELD_MAPPINGS['剩余额度']: prd_info.get('usableAmt', None),
                    FIELD_MAPPINGS['产品起始日期']: prd_info.get('startDate', None),
                    FIELD_MAPPINGS['产品结束日期']: prd_info.get('endDate', None),
                    FIELD_MAPPINGS['下一个开放日']: prd_info.get('nextOpenDate', None),
                    FIELD_MAPPINGS['运作模式']: prd_info.get('prdTypeName', None),
                    # 业绩比较基准一般用于净值型产品
                    FIELD_MAPPINGS['业绩比较基准']: json.dumps({
                                                             'title': prd_info['divModesName'].split(':')[1].split('(')[
                                                                 0],
                                                             'qsrq': (re.findall(PATTERN_Z,
                                                                                 prd_info['divModesName']) + re.findall(
                                                                 PATTERN_E, prd_info['divModesName']))[0].split('至')[0],
                                                             'jsrq': (re.findall(PATTERN_Z,
                                                                                 prd_info['divModesName']) + re.findall(
                                                                 PATTERN_E, prd_info['divModesName']))[0].split('至')[1],
                                                             'jz': prd_info['divModesName'].split(':')[0]
                                                         } if prd_info.get('prdTypeName', '') == '净值类周期型'
                                                              and len(prd_info['divModesName'].split(':')) >= 2 \
                                                              and len((re.findall(PATTERN_Z, prd_info[
                        'divModesName']) + re.findall(PATTERN_E, prd_info['divModesName']))) >= 1 \
                                                             else '').encode().decode('unicode_escape'),
                    FIELD_MAPPINGS['净值']: prd_info.get('NAV', None),
                    FIELD_MAPPINGS['净值日期']: prd_info.get('navDate', None),
                    FIELD_MAPPINGS['管理人']: prd_info.get('prdManagerName', None),
                }
                keys = [k for k in row.keys()]
                for key in keys:
                    if not row[key]:
                        del row[key]
                print(row)
                rows.append(row)
        return rows
    except Exception as e:
        exception = cast_exception(e)
        raise exception


if __name__ == '__main__':
    session = requests.session()
    process_pc(session)
    session.close()
