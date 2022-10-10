import json
import math
import time
from typing import List

import requests
from requests import Response

from config_parser import crawl_config
from crawl_utils.common_utils import delete_empty_value
from crawl_utils.crawl_request import CustomCrawlRequest, SessionRequest
from crawl_utils.db_utils import getLocalDate
from crawl_utils.logging_utils import get_logger
from crawl_utils.mappings import FIELD_MAPPINGS
from 中国民生银行_完成.zgmsyh_config import MASK, SLEEP_SECOND
from zgmsyh_assist import mobile_filter1, mobile_filter2, mobile_filter3

logger = get_logger(name=__name__, log_level=crawl_config.log_level, log_modules=crawl_config.log_modules,
                    filename=crawl_config.log_filename)


class ZgmsyhMobileCrawlRequest(CustomCrawlRequest):
    def __init__(self):
        super(ZgmsyhMobileCrawlRequest, self).__init__(
            name='中国光大银行',
            session=requests.session(),
            config=crawl_config,
            check_props=['logId', 'cpbm','mark'],
            mask=MASK
        )
        self.page_no = None
        self.total_page = None
        self.log_id = None
        self.filters.append(mobile_filter1)
        self.filters.append(mobile_filter2)
        self.filters.append(mobile_filter3)

    def set_request_json(self, page_no):
        self.request.json = {
            "request": {
                "header": {
                    "appId": "",
                    "appVersion": "",
                    "device": {
                        "osType": "BROWSER",
                        "osVersion": "",
                        "uuid": ""
                    }
                },
                "body": {
                    "pageSize": 10,
                    "currentIndex": (page_no - 1) * 10,
                    "pageNo": page_no,
                    "orderFlag": "6",
                    "liveTime": "3",
                    "isKJTSS": "0",
                    "prdAttr": "0",
                    "prdChara": "4",
                    "prdTypeNameList": [],
                    "fundModeList": [],
                    "pfirstAmtList": [],
                    "currTypeList": []
                }
            }
        }

    def _pre_crawl(self):
        self.request.url = 'https://ment.cmbc.com.cn/gw/pwx_wx/QryProdListOnMarket.do'
        self.request.method = 'POST'
        self.request.headers.update({
            "Host": "ment.cmbc.com.cn",
            "Origin": "https//ment.cmbc.com.cn",
            "Referer": "https//ment.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/finance/selling-list",
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36"
        })

    def _config_params(self):
        """
        设置page_no
        :return:
        """
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.set_request_json(self.page_no)

    def _parse_response(self, response: Response) -> List[dict]:
        response_str = response.text \
            .encode(response.encoding) \
            .decode('utf-8') \
            if response.encoding \
            else response.text
        loads = json.loads(response_str)
        if self.total_page is None:
            total_size = loads['response']['totalSize']
            total_page = math.ceil(total_size / len(loads['response']['prdList']))
            self.total_page = total_page
        if self.crawl_config.state == 'DEV':
            self.total_page = 1
        return loads['response']['prdList']

    def get_detail_info(self, row):
        # 处理详情页
        detail_url = 'https://ment.cmbc.com.cn/gw/pwx_wx/QueryPrdBuyInfo.do'
        detail_headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Length": "185",
            "Content-Type": "application/json;charset=UTF-8",
            "Cookie": "OUTFOX_SEARCH_USER_ID_NCOO=2040096739.6771917; PWX_WX_SESSIONID=34FC7B1B273C572F20EB1F96F27C0193; org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE=zh_CN; BIGipServerUEB_tongyidianziqudao_app_41002_pool=!L2XyF0+cO/ssrkw0lXP1ySZhZOpxgnxVuEYnVLA5Hg4K7elqXQ998Lleke6jYiFBhfQpPyRSaWBl7A==",
            "Host": "ment.cmbc.com.cn",
            "Origin": "https//ment.cmbc.com.cn",
            "Pragma": "no-cache",
            "Referer": "https//ment.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/finance/selling-detail?prdCode=FSAE68205A&prdType=4&startDate=2022-03-08",
            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
            "sec-ch-ua-mobile": "?1",
            "sec-ch-ua-platform": "\"Android\"",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36"}
        detail_data = {
            "request": {
                "header": {
                    "appId": "", "appVersion": "",
                    "device": {
                        "osType": "BROWSER",
                        "osVersion": "",
                        "uuid": ""
                    }
                },
                "body": {
                    "prdCode": row['cpbm'],
                    "getVipFlag": "1",
                    "GroupFlag": "1",
                    "isKJTSS": "0"
                }
            }
        }
        detail_response = self.session.post(url=detail_url, json=detail_data, headers=detail_headers)
        time.sleep(SLEEP_SECOND)
        detail_resp_str = detail_response.text.encode(
            detail_response.encoding) if detail_response.encoding else detail_response.text
        detail_loads = json.loads(detail_resp_str)['response']
        detail_row = {
            FIELD_MAPPINGS['净值']: str(detail_loads.get('NAV', '')),
            FIELD_MAPPINGS['产品结束日期']: detail_loads.get('endDate', ''),
            FIELD_MAPPINGS['销售渠道']: json.dumps(detail_loads.get('channelsName', '').split(';')).encode().decode(
                'unicode_escape') if detail_loads.get('channelsName', '') else '',
            FIELD_MAPPINGS['产品编码']: detail_loads.get('prdCode', ''),
            FIELD_MAPPINGS['产品状态']: detail_loads.get('statusName', ''),
            FIELD_MAPPINGS['币种']: detail_loads.get('currTypeName', ''),
            FIELD_MAPPINGS['产品简称']: detail_loads.get('prdShortName', ''),
            FIELD_MAPPINGS['委托人']: detail_loads.get('prdTrusteeName', ''),
            FIELD_MAPPINGS['募集结束日期']: detail_loads.get('ipoEndDate', ''),
            FIELD_MAPPINGS['募集起始日期']: detail_loads.get('ipoStartDate', ''),
            FIELD_MAPPINGS['风险等级']: detail_loads.get('riskLevelName', ''),
            FIELD_MAPPINGS['赎回规则']: detail_loads.get('warmTipsMap', {}).get('tipsTitle', ''),
            FIELD_MAPPINGS['投资期限']: detail_loads.get('livTimeUnitName', '')
        }
        # 删除detail_row中value为空的key
        delete_empty_value(detail_row)
        return detail_row

    def _process_rows(self, rows: List[dict]) -> List[dict]:
        for row in rows:
            # 获取详情页数据信息
            detail_row = self.get_detail_info(row)
            row.update(detail_row)
            row['logId'] = self.log_id
            row['createTime'] = getLocalDate()
            row['mark'] = 'MOBILE'
        return rows

    def _config_end_flag(self):
        if self.page_no and self.total_page and self.page_no == self.total_page:
            self.end_flag = True
