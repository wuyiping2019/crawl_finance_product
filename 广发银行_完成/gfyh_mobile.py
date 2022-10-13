import json
from typing import List
from requests import Response
from crawl_utils.common_utils import delete_empty_value
from crawl_utils.crawl_request import ConfigurableCrawlRequest
from crawl_utils.db_utils import getLocalDate
from 广发银行_完成.gfyh_config import MASK, MOBILE_FIELD_VALUE_MAPPING, MOBILE_FIELD_NAME_2_NEW_FIELD_NAME, \
    MOBILE_REQUEST_URL, MOBILE_REQUEST_METHOD, MOBILE_REQUEST_HEADERS, MOBILE_REQUEST_JSON


class GfyhMobileCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super(GfyhMobileCrawlRequest, self).__init__(name="广发银行MOBILE")
        self.page_no = None
        self.gain_rs = None

    def _pre_crawl(self):
        self.mask = MASK
        # 爬虫之前的配置工作
        self.url = MOBILE_REQUEST_URL
        self.method = MOBILE_REQUEST_METHOD
        self.headers = MOBILE_REQUEST_HEADERS
        # 配置过滤器
        self.field_value_mapping = MOBILE_FIELD_VALUE_MAPPING
        self.field_name_2_new_field_name = MOBILE_FIELD_NAME_2_NEW_FIELD_NAME

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.json = MOBILE_REQUEST_JSON(self.page_no)

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


if __name__ == '__main__':
    GfyhMobileCrawlRequest().init_props(log_id=1).do_crawl()

