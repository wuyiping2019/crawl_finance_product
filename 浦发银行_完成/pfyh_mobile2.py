import json
import time
from typing import List

from requests import Response

from crawl_utils.crawl_request import ConfigurableCrawlRequest, RowFilter
from crawl_utils.db_utils import getLocalDate
from 浦发银行_完成.pfyh_config import MOBILE_REQUEST_URL, MOBILE_REQUEST_HEADERS, MOBILE_REQUEST_DATA, MOBILE_REQUEST_METHOD, \
    MASK, MOBILE_DETAIL_REQUEST_METHOD, MOBILE_DETAIL_REQUEST_URL, MOBILE_DETAIL_REQUEST_DATA, \
    MOBILE_FIELD_NAME_2_NEW_FIELD_NAME, MOBILE_DETAIL_REQUEST_HEADERS


class PfyhMobileCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super(PfyhMobileCrawlRequest, self).__init__(name='浦发银行MOBILE')
        self.page_no = None
        self.total_page = None

    def _pre_crawl(self):
        self.url = MOBILE_REQUEST_URL
        self.headers = MOBILE_REQUEST_HEADERS
        self.method = MOBILE_REQUEST_METHOD
        self.mask = MASK
        self.field_name_2_new_field_name = MOBILE_FIELD_NAME_2_NEW_FIELD_NAME

        # 添加处理详情页的filter
        row_filter = RowFilter().set_name('get_detail_info')
        row_filter.filter = self.get_detail_info
        self.add_filter_after_row_processor(row_filter)

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.data = json.dumps(MOBILE_REQUEST_DATA(self.page_no))
        if self.config.state == 'DEV':
            self.total_page = 1

    def _parse_response(self, response: Response) -> List[dict]:
        loads = json.loads(response.text)
        if self.total_page is None:
            self.total_page = loads['RspBody']['Finance_Num']
        return loads['RspBody']['List']

    def _config_end_flag(self):
        if self.page_no and self.total_page and self.page_no == self.total_page:
            self.end_flag = True

    def _row_processor(self, row: dict) -> dict:
        return row

    def _row_post_processor(self, row: dict) -> dict:
        row['logId'] = self.log_id
        row['createTime'] = getLocalDate()
        row['mark'] = 'MOBILE'
        row['bank'] = '浦发银行'
        return row

    def get_detail_info(self, row: dict):
        response = self.session.request(
            method=MOBILE_DETAIL_REQUEST_METHOD,
            url=MOBILE_DETAIL_REQUEST_URL,
            data=json.dumps(MOBILE_DETAIL_REQUEST_DATA(row['Finance_No'])),
            headers=MOBILE_DETAIL_REQUEST_HEADERS
        )
        time.sleep(1)
        detail_row = json.loads(response.text)['RspBody']
        print(detail_row)
        row.update(detail_row)
        return row


if __name__ == '__main__':
    PfyhMobileCrawlRequest().init_props(log_id=1).do_crawl()
