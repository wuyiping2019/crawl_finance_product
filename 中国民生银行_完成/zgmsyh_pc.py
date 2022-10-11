import json
import math
from typing import List

import requests
from requests import Response

from config_parser import crawl_config
from crawl_utils.crawl_request import CustomCrawlRequest

# http://www.cmbc.com.cn/xmhpd/grkh/rmcp/rmlc/index.htm
from crawl_utils.db_utils import getLocalDate
from 中国民生银行_完成.zgmsyh_assist import pc_filter1, pc_filter2, pc_filter3
from 中国民生银行_完成.zgmsyh_config import MASK


class ZgmsyhPCCrawlRequest(CustomCrawlRequest):
    def __init__(self):
        super(ZgmsyhPCCrawlRequest, self).__init__(
            name='中国民生银行',
            session=requests.session(),
            config=crawl_config,
            check_props=['logId', 'cpbm','mark'],
            mask=MASK
        )
        self.page_no = None
        self.total_page = None
        self.log_id = None
        self.filters.append(pc_filter1)
        self.filters.append(pc_filter2)
        self.filters.append(pc_filter3)

    def _process_rows(self, rows: List[dict]) -> List[dict]:
        for row in rows:
            row['logId'] = self.log_id
            row['createTime'] = getLocalDate()
            row['mark'] = 'PC'
        return rows

    def _pre_crawl(self):
        self.request.url = 'http://www.cmbc.com.cn/gw/po_web/QryProdListOnMarket.do'
        self.request.method = 'POST'
        self.request.headers.update(
            {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Content-Type": "application/json;charset=UTF-8",
                "Host": "www.cmbc.com.cn",
                "Origin": "http//www.cmbc.com.cn",
                "Referer": "http//www.cmbc.com.cn/xmhpd/grkh/rmcp/rmlc/index.htm",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
                "X-Requested-With": "XMLHttpRequest"
            }
        )

    def set_request_json(self, page_no):
        self.request.json = {
            "currTypeList": [],
            "keyWord": "",
            "currentIndex": 0,
            "fundModeList": [],
            "orderFlag": "1",
            "pageNo": page_no,
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

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.set_request_json(self.page_no)

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        loads = json.loads(resp_str)
        if self.total_page is None:
            self.total_page = math.ceil(loads['totalSize'] / len(loads['prdList']))
            if self.crawl_config.state == 'DEV':
                self.total_page = 1
        return loads['prdList']

    def _config_end_flag(self):
        if self.page_no and self.total_page and self.page_no == self.total_page:
            self.end_flag = True
