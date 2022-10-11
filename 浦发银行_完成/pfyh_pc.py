import json
from typing import List

from requests import Response

from crawl_utils.crawl_request import ConfigurableCrawlRequest
from crawl_utils.db_utils import getLocalDate
from 浦发银行_完成.pfyh_config import PC_REQUEST_URL, PC_REQUEST_METHOD, PC_REQUEST_HEADERS, PC_REQUEST_DATA, \
    PC_FIELD_VALUE_MAPPING, PC_FIELD_NAME_2_NEW_FIELD_NAME, MASK


class PfyhPCCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super(PfyhPCCrawlRequest, self).__init__(name='浦发银行PC')
        self.page_no = None
        self.total_page = None

    def _pre_crawl(self):
        self.mask = MASK
        self.request['url'] = PC_REQUEST_URL
        self.request['method'] = PC_REQUEST_METHOD
        self.request['headers'] = PC_REQUEST_HEADERS
        self.field_value_mapping = PC_FIELD_VALUE_MAPPING
        self.field_name_2_new_field_name = PC_FIELD_NAME_2_NEW_FIELD_NAME

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.request['data'] = PC_REQUEST_DATA(self.page_no)

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        loads = json.loads(resp_str)
        if self.total_page is None:
            self.total_page = loads['pageTotal']
            if self.crawl_config.state == 'DEV':
                self.total_page = 1
        return loads['rows']

    def _config_end_flag(self):
        if self.page_no is not None and self.total_page is not None and self.page_no == self.total_page:
            self.end_flag = True

    def _row_processor(self, row: dict) -> dict:
        return row

    def _row_post_processor(self, row: dict):
        row['logId'] = self.log_id
        row['createTime'] = getLocalDate()
        row['mark'] = 'PC'
        row['bank'] = '浦发银行'
        return row
