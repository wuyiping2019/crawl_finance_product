import json
import math
from typing import List

from requests import Response

from crawl_utils.common_utils import delete_empty_value
from crawl_utils.crawl_request import ConfigurableCrawlRequest

####################
# https://ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true
####################
from crawl_utils.db_utils import getLocalDate
from 平安银行_完成.payh_config import PC_REQUESTS_ITER, FIELD_VALUE_MAPPING


class PayhPCCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super().__init__()
        self.request_iter_index = None
        self.page_no = None
        self.total_page = None

    def _pre_crawl(self):
        self.request_iter_index = 0
        if self.crawl_config.state == 'DEV':
            self.total_page = 1
        self.field_value_mapping = FIELD_VALUE_MAPPING

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.field_name_2_new_field_name = PC_REQUESTS_ITER[self.request_iter_index]['field_name_2_new_field_name']
        self.check_props = PC_REQUESTS_ITER[self.request_iter_index]['check_props']
        self.mask = PC_REQUESTS_ITER[self.request_iter_index]['identifier']
        for k, v in PC_REQUESTS_ITER[self.request_iter_index]['request'].items():
            if not hasattr(v, '__call__'):
                self.request[k] = v
        self.request['data'] = PC_REQUESTS_ITER[self.request_iter_index]['request']['data'](self.page_no)

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text.encode(response.encoding).decode('utf-8', errors='ignore') \
            if response.encoding else response.text
        loads = json.loads(resp_str)
        rows = loads['data']['superviseProductInfoList']
        if self.total_page is None:
            self.total_page = math.ceil(int(float(loads['data']['totalSize'])) / len(rows))
        return rows

    def _config_end_flag(self):
        if self.page_no is not None and self.total_page is not None and self.page_no == self.total_page:
            if self.request_iter_index + 1 == len(PC_REQUESTS_ITER):
                self.end_flag = True
            else:
                self.request_iter_index += 1
                self.page_no = None
                if self.crawl_config.state == 'DEV':
                    self.total_page = 1
                else:
                    self.total_page = None

    def _row_processor(self, row: dict) -> dict:
        return row

    def _row_post_processor(self, row: dict):
        row['logId'] = self.log_id
        row['createTime'] = getLocalDate()
        row['mark'] = 'PC'
        row['ywfl'] = PC_REQUESTS_ITER[self.request_iter_index]['title']
        delete_empty_value(row)
        return row


if __name__ == '__main__':
    crawl_pc = PayhPCCrawlRequest()
    crawl_pc.do_crawl()
