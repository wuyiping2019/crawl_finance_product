import json
import math
from typing import List
from requests import Response
from crawl_utils.crawl_request import ConfigurableCrawlRequest
from crawl_utils.db_utils import getLocalDate
from 中国民生银行_完成.zgmsyh_config import MASK, PC_REQUEST_JSON, PC_FIELD_VALUE_MAPPINGS, PC_FIELD_NAME_2_NEW_FIELD_NAME, \
    PC_REQUEST_URL, PC_REQUEST_METHOD


# http://www.cmbc.com.cn/xmhpd/grkh/rmcp/rmlc/index.htm
class ZgmsyhPCCrawlRequest(ConfigurableCrawlRequest):
    def _row_processor(self, row: dict) -> dict:
        return row

    def __init__(self):
        super(ZgmsyhPCCrawlRequest, self).__init__(name='中国民生银行PC')
        self.page_no = None
        self.total_page = None

    def _row_post_processor(self, row: dict) -> dict:
        row['logId'] = self.log_id
        row['bank'] = '中国民生银行'
        row['mark'] = 'PC'
        row['createTime'] = getLocalDate()
        return row

    def _pre_crawl(self):
        self.mask = MASK
        self.url = PC_REQUEST_URL
        self.method = PC_REQUEST_METHOD
        headers = self.default_request_headers.copy()
        self.headers = headers.update(

        )
        if self.crawl_config.state == 'DEV':
            self.total_page = 1

        # 过滤器
        self.field_value_mapping = PC_FIELD_VALUE_MAPPINGS
        self.field_name_2_new_field_name = PC_FIELD_NAME_2_NEW_FIELD_NAME

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.json = PC_REQUEST_JSON(self.page_no)

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        loads = json.loads(resp_str)
        if self.total_page is None:
            self.total_page = math.ceil(loads['totalSize'] / len(loads['prdList']))
        return loads['prdList']

    def _config_end_flag(self):
        if self.page_no and self.total_page and self.page_no == self.total_page:
            self.end_flag = True


if __name__ == '__main__':
    crawl_pc = ZgmsyhPCCrawlRequest().init_props(log_id=1)
    crawl_pc.do_crawl()
    crawl_pc.config.db_pool.close()
