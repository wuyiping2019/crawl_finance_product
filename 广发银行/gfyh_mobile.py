import json
from typing import List
from requests import Response
from utils.crawl_request import AbstractCrawlRequest
from utils.db_utils import getLocalDate
from utils.mappings import FIELD_MAPPINGS


class GfyhCrawlRequest(AbstractCrawlRequest):
    def _prep_request(self):
        response = self.session.request(
            method='post',
            url='https://wap.cgbchina.com.cn/h5-mobilebank-app/noSessionServlet/hbss/fn10026.lgx',
            json={"body": {"channel": "400", "sChannel": "WX", "prdType": "1"},
                  "header": {"senderSN": "1663749151603n1009431", "os": "Win32",
                             "channel": "WX", "secondChannel": "", "scope": "2",
                             "mpSId": "HMBS_C882C49E556385209F40A14EC9972733_1551629178531586048",
                             "utmSource": ""}},
            headers={"host": "wap.cgbchina.com.cn",
                     "accept": "application/json, text/plain, */*",
                     "origin": "https//wap.cgbchina.com.cn",
                     "sendersn": "1663749151603n1009431",
                     "user-agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x6700143B) WindowsWechat(0x6307062c)",
                     "content-type": "application/json;charset=UTF-8",
                     "sec-fetch-site": "same-origin",
                     "sec-fetch-mode": "cors",
                     "sec-fetch-dest": "empty",
                     "referer": "https//wap.cgbchina.com.cn/h5-mobilebank-web/h5/investment/self/list?srcChannel=WX&secondaryChannel=WX&mainChannel=400&tab=1&srcScene=GFYHGZH&channel=400&sChannel=MB&faceFlag=LS&isRegistCS=1&HMBA_STACK_HASH=1663748433050",
                     "accept-encoding": "gzip, deflate, br",
                     "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7"}
        )
        loads = json.loads(response.text
                           .encode(response.encoding)
                           .decode('utf-8', errors='ignore')
                           if response.encoding
                           else response.text)['body']
        for k, v in loads.items():
            if k not in self.field_value_mapping.keys():
                k = k[:-4]
                self.field_value_mapping[k] = {}
            for item in v:
                if k == 'curType':
                    self.field_value_mapping[k][item['curType']] = item['curTypeName']
                elif k == 'expireType':
                    self.field_value_mapping[k][item['expireType']] = item['expireTypeName']
                elif k == 'firstAmt':
                    self.field_value_mapping[k][item['firstAmt']] = item['firstAmtName']
                elif k == 'investManager':
                    self.field_value_mapping[k][item['investManagerNo']] = item['investManagerName']
                elif k == 'prdLimit':
                    self.field_value_mapping[k][item['limitTime']] = item['limitTimeName']
                elif k == 'prdSellStatus':
                    self.field_value_mapping[k][item['prdSellStatus']] = item['prdSellStatusName']
                elif k == 'riskLevel':
                    self.field_value_mapping[k][item['riskLevel']] = item['riskLevelName']
        self._prep_request_flag = True

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        return json.loads(resp_str)['body']['list']

    def _row_processor(self, row: dict) -> dict:
        return row

    def _if_end(self, response: Response) -> bool:
        parse_response = self._parse_response(response)
        if len(parse_response) == 0:
            return True

    def _row_post_processor(self, row: dict) -> dict:
        if {'yieldName', 'yieldName2', 'yieldVal2'}.issubset(row.keys()):
            if row['yieldName'] != '单位净值':
                row['yjbjjz'] = json.dumps({
                    'title': row['yieldName'],
                    'value': row['yieldVal2']
                }).encode().decode('unicode_escape')
            else:
                row['yjbjjz'] = json.dumps({
                    'title': '预期收益率',
                    'value': row['yieldVal2']
                }).encode().decode('unicode_escape')
        for key in {'yieldName', 'yieldName2', 'yieldVal2'}:
            if key in row.keys():
                del row[key]
        row['logId'] = self.log_id
        row['createTime'] = getLocalDate()
        return row

    def get_json(self, page_no: int):
        return {
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

    def _next_request(self):
        # 需要记录一个递增的变量
        if 'page_no' not in self.kwargs.keys():
            self.kwargs['page_no'] = 1
        else:
            self.kwargs['page_no'] += 1
        self.request['json'] = self.get_json(self.kwargs['page_no'])


gfyh_crawl_mobile = GfyhCrawlRequest(
    # 请求参数
    request={
        'url': 'https://wap.cgbchina.com.cn/h5-mobilebank-app/noSessionServlet/hbss/fn20027.lgx',
        'headers': {
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
        },
        'method': 'post'
    },
    identifier='gfyh',
    field_value_mapping={
        'issPrice': lambda x: str(x) + '元',
        'prdAttr': lambda x: '非保本浮动收益类' if str(x) == "1" else '保本浮动收益类'
    },
    filed_name_2_new_field_name={
        'prdName': FIELD_MAPPINGS['产品名称'],
        'prdName2': FIELD_MAPPINGS['产品简称'],
        'prdCode': FIELD_MAPPINGS['产品编码'],
        'tACode': FIELD_MAPPINGS['TA编码'],
        'tAName': FIELD_MAPPINGS['TA名称'],
        'riskLevel': FIELD_MAPPINGS['风险等级'],
        'investManagerName': FIELD_MAPPINGS['管理人'],
        'issPrice': FIELD_MAPPINGS['发行价'],
        'iPOEndDate': FIELD_MAPPINGS['募集结束日期'],
        'iPOStartDate': FIELD_MAPPINGS['募集起始日期'],
        'estabDate': FIELD_MAPPINGS['产品起始日期'],
        'endDate': FIELD_MAPPINGS['产品结束日期'],
        'curType': FIELD_MAPPINGS['币种'],
        'nAV': FIELD_MAPPINGS['净值'],
        'nAVDate': FIELD_MAPPINGS['净值日期'],
        'totLimitStr': FIELD_MAPPINGS['总额度'],
        'yieldName': 'yieldName',
        'yieldName2': 'yieldName2',
        'yieldVal2': 'yieldVal2'
    },
    check_props=['logId', 'cpbm']
)

__all__ = ['gfyh_crawl_mobile']
