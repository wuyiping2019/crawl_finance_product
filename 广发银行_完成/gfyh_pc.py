import re
import types
from typing import List

from bs4 import BeautifulSoup
from requests import Response
from crawl_utils.crawl_request import ConfigurableCrawlRequest
from crawl_utils.db_utils import getLocalDate
from crawl_utils.html_utils import parse_table
from 广发银行_完成.gfyh_assist import pc_requests_iter


class GfyhPCCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super(GfyhPCCrawlRequest, self).__init__()
        self.requests_iter = pc_requests_iter
        self.page_no = None
        self.total_page = None
        self.current_request_index = 0
        self.total_request_cycle = len(self.requests_iter)

    def _pre_crawl(self):
        pass

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 0
        else:
            self.page_no += 1
        for k, v in self.requests_iter[self.current_request_index]['request'].items():
            if isinstance(v, types.LambdaType):
                pass
            else:
                self.request[k] = v
        self.field_name_2_new_field_name = self.requests_iter[self.current_request_index]['field_name_2_new_field_name']
        self.field_value_mapping = self.requests_iter[self.current_request_index]['field_value_mapping']
        self.check_props = self.requests_iter[self.current_request_index]['check_props']
        self.mask = self.requests_iter[self.current_request_index]['mask']
        self.request['json'] = self.requests_iter[self.current_request_index]['request']['json'](self.page_no)

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.content.decode(response.encoding) \
            if response.encoding \
            else response.text
        if self.total_page is None:
            # 没有设置总页数
            self.total_page = re.findall(r'第\d/(\d+)页', resp_str)
            if self.crawl_config.state == 'DEV':
                self.total_page = 1
        soup = BeautifulSoup(resp_str, 'lxml')
        tables = soup.select('table')
        if tables:
            table = tables[0]
            rows = parse_table(table,
                               None,
                               callbacks={},
                               extra_attrs={})
            return rows
        else:
            []

    def _config_end_flag(self):
        # 至少执行一次请求解析 设置total_page之后 _if_end才能够判断是否达到终止条件
        if self.total_page is not None:
            if self.page_no >= self.total_page:
                # 表示当前请求结束之后需要切换url或终止
                if self.current_request_index + 1 == self.total_request_cycle:
                    self.end_flag = True
                else:
                    self.current_request_index += 1

    def _row_processor(self, row: dict) -> dict:
        return row

    def _row_post_processor(self, row: dict):
        row['logId'] = self.log_id
        row['createTime'] = getLocalDate()
        if 'cpbm' not in row.keys():
            row['cpbm'] = row['cpmc']
        row['ywfl'] = self.requests_iter[self.current_request_index]['title']
        row['mark'] = 'PC'
        return row
