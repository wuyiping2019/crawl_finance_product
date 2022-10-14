import json
from typing import List

from requests import Response

from crawl_utils.crawl_request import ConfigurableCrawlRequest
from crawl_utils.custom_exception import CustomException
from crawl_utils.db_utils import getLocalDate
from crawl_utils.mappings import FIELD_MAPPINGS
from 华夏银行_完成.hxyh_config import MASK, MOBILE_FIELD_NAME_2_NEW_FIELD_NAME, \
    MOBILE_FIELD_VALUE_MAPPING, MOBILE_REQUEST_JSON, MOBILE_REQUEST_URL, MOBILE_REQUEST_METHOD


class HxyhMobileCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super().__init__(name='华夏银行MOBILE')
        self.page_no = None
        self.total_page = None

    def _pre_crawl(self):
        self.mask = MASK
        self.url = MOBILE_REQUEST_URL
        self.method = MOBILE_REQUEST_METHOD
        self.field_name_2_new_field_name = MOBILE_FIELD_NAME_2_NEW_FIELD_NAME
        self.field_value_mapping = MOBILE_FIELD_VALUE_MAPPING
        self.check_props = ['logId', 'cpbm', 'bank']
        if self.crawl_config.state == 'DEV':
            self.total_page = 1

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.json = MOBILE_REQUEST_JSON(self.page_no)

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
        row['bank'] = '华夏银行'
        return row


if __name__ == '__main__':
    HxyhMobileCrawlRequest().init_props(log_id=1).do_crawl()
