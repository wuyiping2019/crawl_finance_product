import json
from typing import List

from requests import Response

from crawl_utils.crawl_request import ConfigurableCrawlRequest
from crawl_utils.custom_exception import CustomException
from crawl_utils.db_utils import getLocalDate
from crawl_utils.mappings import FIELD_MAPPINGS
from 华夏银行_完成.hxyh_config import MOBILE_REQUEST_INFO


class HxyhMobileCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super().__init__()
        self.page_no = None
        self.total_page = None
        self.request = {}

    def set_request_json(self):
        self.request['json'] = {
            "request": {
                "sortField": "",
                "sortType": "",
                "type": "",
                "productName": "",
                "page": self.page_no,
                "hidderLoading": True
            }
        }

    def _pre_crawl(self):
        self.mask = MOBILE_REQUEST_INFO['identifier']
        self.field_name_2_new_field_name = MOBILE_REQUEST_INFO['field_name_2_new_field_name']
        self.check_props = MOBILE_REQUEST_INFO['check_props']
        self.request = MOBILE_REQUEST_INFO['request']

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.set_request_json()
        if self.crawl_config.state == 'DEV':
            self.total_page = 1

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        loads = json.loads(resp_str)
        loads_response = loads['response']
        if isinstance(loads_response, str):
            loads_response = json.loads(loads_response)
        elif isinstance(loads_response, dict):
            pass
        if self.total_page is None:
            self.total_page = int(float(loads_response['data']['totalPage']))
        return loads_response['data']['list']

    def _config_end_flag(self):
        if self.page_no is not None and self.total_page is not None and self.page_no == self.total_page:
            self.end_flag = True

    def _row_processor(self, row: dict) -> dict:
        # 处理业绩比较基准
        income_type = row.get('incomeType', '')
        if income_type == '1':
            row[FIELD_MAPPINGS['业绩比较基准']] = json.dumps({
                'title': '预期年化收益率',
                'value': row['productExpectIncomeRate'] + '%' if row['productExpectIncomeRate'] else ''
            }).encode().decode('unicode_escape')
        else:
            raise CustomException(None, f'出现没有考虑的{income_type}')
        return row

    def _row_post_processor(self, row: dict):
        row['logId'] = self.log_id
        row['createTime'] = getLocalDate()
        row['mark'] = 'MOBILE'
        return row


if __name__ == '__main__':
    crawl_mobile = HxyhMobileCrawlRequest()
    crawl_mobile.do_crawl()
