import json
import math
import time
from typing import List

import requests
from requests import Response

from config_parser import crawl_config
from crawl_utils.common_utils import delete_empty_value
from crawl_utils.crawl_request import ConfigurableCrawlRequest, RowFilter, RowKVTransformAndFilter
from crawl_utils.db_utils import getLocalDate
from crawl_utils.logging_utils import get_logger
from crawl_utils.mappings import FIELD_MAPPINGS
from 中国民生银行_完成.zgmsyh_config import MASK, SLEEP_SECOND, MOBILE_FIELD_VALUE_MAPPING, MOBILE_FIELD_NAME_2_NEW_FIELD_NAME, \
    MOBILE_REQUEST_HEADERS, MOBILE_REQUEST_METHOD, MOBILE_REQUEST_URL, MOBILE_DETAIL_REQUEST_METHOD, \
    MOBILE_DETAIL_REQUEST_URL, MOBILE_DETAIL_REQUEST_JSON, MOBILE_DETAIL_VALUE_FIELD_MAPPING, \
    MOBILE_DETAIL_FIELD_NAME_2_NEW_FIELD_NAME

logger = get_logger(name=__name__)


class ZgmsyhMobileCrawlRequest(ConfigurableCrawlRequest):
    def _row_processor(self, row: dict) -> dict:
        return row

    def _row_post_processor(self, row: dict) -> dict:
        row['logId'] = self.log_id
        row['createTime'] = getLocalDate()
        row['mark'] = 'MOBILE'
        row['bank'] = '中国民生银行'
        return row

    def __init__(self):
        super(ZgmsyhMobileCrawlRequest, self).__init__(name='中国光大银行MOBILE')
        self.page_no = None
        self.total_page = None

    def set_request_json(self, page_no):
        self.json = {
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
        self.mask = MASK
        self.check_props = ['logId', 'cpbm', 'bank']
        # 设置过滤器
        self.field_value_mapping = MOBILE_FIELD_VALUE_MAPPING
        self.field_name_2_new_field_name = MOBILE_FIELD_NAME_2_NEW_FIELD_NAME
        # 设置请求参数
        self.url = MOBILE_REQUEST_URL
        self.method = MOBILE_REQUEST_METHOD
        MOBILE_REQUEST_HEADERS.update(self.default_request_headers)
        self.headers = MOBILE_REQUEST_HEADERS
        if self.config.state == 'DEV':
            self.total_page = 1
        # 添加处理详情页的过滤器
        row_filter = RowFilter()
        row_filter.filter = self.get_detail_info
        # 将该过滤器添加到过滤器链的row_processor之后
        self.add_filter_after_row_processor(row_filter)

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
        return loads['response']['prdList']

    def get_detail_info(self, row):
        # 处理详情页
        detail_response = self.session.request(
            method=MOBILE_DETAIL_REQUEST_METHOD,
            url=MOBILE_DETAIL_REQUEST_URL,
            json=MOBILE_DETAIL_REQUEST_JSON(row['prdCode']),
            headers=MOBILE_REQUEST_HEADERS
        )
        time.sleep(SLEEP_SECOND)
        detail_resp_str = detail_response.text.encode(
            detail_response.encoding) if detail_response.encoding else detail_response.text
        detail_loads = json.loads(detail_resp_str)['response']
        value_field_mapping_filter = RowKVTransformAndFilter(filters=MOBILE_DETAIL_VALUE_FIELD_MAPPING)
        field_name_2_new_field_name_filter = RowKVTransformAndFilter(filters=MOBILE_DETAIL_FIELD_NAME_2_NEW_FIELD_NAME)
        row = value_field_mapping_filter.filter(row)
        row = field_name_2_new_field_name_filter.filter(row)
        return row

    def _config_end_flag(self):
        if self.page_no and self.total_page and self.page_no == self.total_page:
            self.end_flag = True


if __name__ == '__main__':
    crawl_mobile = ZgmsyhMobileCrawlRequest().init_props(log_id=1)
    crawl_mobile.do_crawl()
