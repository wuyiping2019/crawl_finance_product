import json
import re
from typing import List
from requests import Response
from crawl_utils.crawl_request import ConfigurableCrawlRequest
from crawl_utils.db_utils import getLocalDate
from crawl_utils.mappings import FIELD_MAPPINGS
from 招商银行_完成.zsyh_config import PC_REQUEST_URL, PC_REQUEST_HEADERS, PC_REQUEST_PARAMS, MASK


class ZsyhPCCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super(ZsyhPCCrawlRequest, self).__init__(name='招商银行PC端')
        self.page_no = None
        self.total_page = None

    def _pre_crawl(self):
        self.mask = MASK
        self.check_props = ['logId', 'cpbm', 'bank']
        self.request['url'] = PC_REQUEST_URL
        self.request['headers'] = PC_REQUEST_HEADERS
        self.request['method'] = 'GET'
        self.field_value_mapping = {

        }
        self.field_name_2_new_field_name = {
            'PrdCode': FIELD_MAPPINGS['产品编码'],
            'PrdName': FIELD_MAPPINGS['产品名称'],
            'PrdBrief': FIELD_MAPPINGS['产品简称'],
            'funCod': FIELD_MAPPINGS['产品编码'],
            'AreaCode': FIELD_MAPPINGS['销售区域'],
            'Currency': FIELD_MAPPINGS['币种'],
            'BeginDate': FIELD_MAPPINGS['募集起始日期'],
            'EndDate': FIELD_MAPPINGS['募集结束日期'],
            'ShowExpireDate': FIELD_MAPPINGS['产品到期日'],
            'Status': FIELD_MAPPINGS['产品状态'],
            'NetValue': FIELD_MAPPINGS['净值'],
            'Term': FIELD_MAPPINGS['投资期限'],
            'InitMoney': FIELD_MAPPINGS['起购金额'],
            'Risk': FIELD_MAPPINGS['风险等级'],
            'SaleChannelName': FIELD_MAPPINGS['销售渠道'],
            'ShowExpectedReturn': FIELD_MAPPINGS['业绩比较基准'],
            'REGCode': FIELD_MAPPINGS['登记编码'],
            'Style': FIELD_MAPPINGS['投资性质']
        }

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.request['params'] = PC_REQUEST_PARAMS(self.page_no)

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        resp_str = resp_str.replace('(', '').replace(')', '')
        to_json = self.to_json(resp_str)
        loads = json.loads(to_json)
        if self.total_page is None:
            self.total_page = int(loads['totalPage'])
            if self.crawl_config.state == 'DEV':
                self.total_page = 1
        return loads['list']

    def _config_end_flag(self):
        if self.page_no is not None and self.total_page is not None and self.page_no == self.total_page:
            self.end_flag = True

    def _row_processor(self, row: dict) -> dict:
        return row

    def _row_post_processor(self, row: dict):
        row['logId'] = self.log_id
        row['createTime'] = getLocalDate()
        row['bank'] = '招商银行_完成'
        row['mark'] = 'PC'
        return row

    def to_json(self, resp_str: str):
        for pattern in [re.compile(r'{([^".]+?):'), re.compile(r',([^".]+?):')]:
            while True:
                search = pattern.search(resp_str)
                if search is None:
                    break
                start = search.start() + 1
                end = search.end() - 1
                resp_str = resp_str[:start] + f'"{resp_str[start:end]}"' + resp_str[end:]
        return resp_str

    def get_prd_detail_info(self, row):
        # http://www.cmbchina.com/cfweb/Personal/saproductdetail.aspx?saaCod=D07&funCod=9989J&type=prodintro#toTarget
        return row


if __name__ == '__main__':
    crawl_pc = ZsyhPCCrawlRequest().init_props(log_id=1)
    crawl_pc.do_crawl()
    # to_json = crawl_pc.to_json('d:"47586"},{PrdCode:"95024",PrdName:"招银理')
    # print(to_json)
