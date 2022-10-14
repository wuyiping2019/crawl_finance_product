import json
import math
from typing import List

from requests import Response

from crawl_utils.crawl_request import ConfigurableCrawlRequest
from crawl_utils.db_utils import getLocalDate
from 中国理财网.zglcw_config import PC_CITY, PC_REQUEST_URL, PC_REQUEST_METHOD, PC_REQUEST_DATA, PC_REQUEST_HEADERS, MASK


class ZglcwPCCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super(ZglcwPCCrawlRequest, self).__init__(name='中国理财网PC')
        self.page_no = None
        self.total_page = None

    def _pre_crawl(self):
        if self.config.state == 'DEV':
            self.total_page = 1
        self.url = PC_REQUEST_URL
        self.method = PC_REQUEST_METHOD
        self.headers = PC_REQUEST_HEADERS
        self.mask = MASK
        self.check_props = ['logId', 'djbm']

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.data = PC_REQUEST_DATA(self.page_no)

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text
        loads = json.loads(resp_str)
        if self.total_page is None:
            self.total_page = math.ceil(int(loads['Count']) / 500)

        return self.parse_table(resp_str)

    def _config_end_flag(self):
        if self.page_no and self.total_page and self.page_no == self.total_page:
            self.end_flag = True

    def _row_processor(self, row: dict) -> dict:
        return row

    def _row_post_processor(self, row: dict) -> dict:
        row['logId'] = self.log_id
        row['createTime'] = getLocalDate()
        row['mark'] = 'PC'
        row['bank'] = '中国理财网'
        return row

    def parse_table(self, table_str) -> List[dict]:
        """
        解析页面的产品 返回一个产品列表
        :param table_str:
        :return:
        """
        rep = json.loads(table_str)['List']
        rows = []
        for product in rep:
            if ('cptzxzms' in product):
                cptzxzms = product['cptzxzms']
            else:
                cptzxzms = ''
            cpxsqyName = []
            if not product['cpxsqy'] == '':
                cpxsqyList = product['cpxsqy'].split(',')
                for cpxsqy in cpxsqyList:
                    cpxsqyName.append(PC_CITY[cpxsqy])
                cpxsqy = ','.join(cpxsqyName)
            else:
                cpxsqy = '不限'
            row = {
                'djbm': product['cpdjbm'],
                'fxjg': product['fxjgms'],
                'yzms': product['cpyzmsms'],
                'mjfs': product['mjfsms'],
                'qxlx': product['qxms'],
                'mjbz': product['mjbz'],
                'tzxz': cptzxzms,
                'fxdj': product['fxdjms'],
                'mjqsrq': product['mjqsrq'],
                'mjjsrq': product['mjjsrq'],
                'cpqsrq': product['cpqsrq'],
                'cpjsrq': product['cpyjzzrq'],
                'ywqsrq': product['kfzqqsr'],
                'ywjsrq': product['kfzqjsr'],
                'sjts': product['cpqx'],
                'csjz': product['csjz'],
                'cpjz': product['cpjz'],
                'ljjz': product['ljjz'],
                'zjycdfsyl': product['syl'],
                'cptssx': '',
                'tzzclx': product['tzlxms'],
                'dxjg': '',
                'yqzdsyl': product['yjkhzdnsyl'],
                'yqzgsyl': product['yjkhzgnsyl'],
                'yjbjjzxx': product['yjbjjzxx'],
                'yjbjjzsx': product['yjbjjzsx'],
                'xsqy': cpxsqy
            }
            rows.append(row)
        return rows


if __name__ == '__main__':
    ZglcwPCCrawlRequest().init_props(log_id=1).do_crawl()
