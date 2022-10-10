import json
from typing import List

import requests
from requests import Response

from config_parser import crawl_config
from crawl_utils.common_utils import delete_empty_value
from crawl_utils.crawl_request import CustomCrawlRequest, ConfigurableCrawlRequest
from crawl_utils.db_utils import getLocalDate
from 广发银行_完成.gfyh_assist import mobile_filter1, mobile_filter2, mobile_filter3
from 广发银行_完成.gfyh_config import MASK


class GfyhMobileCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super(GfyhMobileCrawlRequest, self).__init__(
            name="广发银行PC端"
        )
        self.page_no = None
        self.gain_rs = None

    def _pre_crawl(self):
        # self.mask = MASK
        # 爬虫之前的配置工作
        self.request.url = 'https://wap.cgbchina.com.cn/h5-mobilebank-app/noSessionServlet/hbss/fn20027.lgx'
        self.request.method = 'POST'
        self.request.headers = {
            'host': 'wap.cgbchina.com.cn',
            'accept': 'application/json, text/plain, */*',
            'origin': 'https://wap.cgbchina.com.cn',
            'sendersn': '1663749151600n2005493',
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x6700143B) WindowsWechat(0x6307062c)',
            'content-type': 'application/json;charset=UTF-8',
            'referer': 'https://wap.cgbchina.com.cn/h5-mobilebank-web/h5/investment/self/list?srcChannel=WX&secondaryChannel=WX&mainChannel=400&tab=1&srcScene=GFYHGZH&channel=400&sChannel=MB&faceFlag=LS&isRegistCS=1&HMBA_STACK_HASH=1663748433050',
        }

    def set_request_json(self):
        self.request.json = {
            "body": {
                "beginNum": (self.page_no - 1) * 20,
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

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.set_request_json()

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        gain_rs = len(json.loads(resp_str)['body']['list'])
        self.gain_rs = gain_rs
        return json.loads(resp_str)['body']['list']

    def _config_end_flag(self):
        if self.gain_rs is not None and self.gain_rs == 0:
            self.end_flag = True
        elif self.crawl_config.state == 'DEV':
            self.end_flag = True

    def _row_processor(self, row: dict) -> dict:
        return row

    def _row_post_processor(self, row: dict) -> dict:
        row['logId'] = self.log_id
        row['createTime'] = getLocalDate()
        row['mark'] = 'MOBILE'
        row['bank'] = '广发银行'
        delete_empty_value(row)
        return row
