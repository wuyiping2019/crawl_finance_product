import json
import math
import re
import time
import types
import typing
from typing import List

from bs4 import BeautifulSoup
from requests import Response

from utils.common_utils import extract_content_between_content
from utils.crawl_request import AbstractCrawlRequest
from utils.custom_exception import CustomException
from utils.db_utils import getLocalDate
from utils.logging_utils import log
from utils.mappings import FIELD_MAPPINGS
from hxyh_config import STATE, SLEEP_SECOND
from utils.string_utils import remove_space


class HxyhCrawlRequest(AbstractCrawlRequest):
    def _update_props(self):
        index = getattr(self, 'current_request_index')
        self.field_name_2_new_field_name = getattr(self, 'requests_iter')[index]['field_name_2_new_field_name']
        self.field_value_mapping = getattr(self, 'requests_iter')[index]['field_value_mapping']
        self.check_props = getattr(self, 'requests_iter')[index]['check_props']
        self.identifier = getattr(self, 'requests_iter')[index]['identifier']

    def _prep_request(self):
        # 初始化爬虫状态
        self.request = {}
        setattr(self, 'requests_iter', self.kwargs['requests_iter'])
        setattr(self, 'current_request_index', 0)
        setattr(self, 'total_request_cycle', len(self.kwargs['requests_iter']))
        setattr(self, 'page_no', None)
        current_iter_request = getattr(self, 'requests_iter')[0]
        for k, v in current_iter_request['request'].items():
            if isinstance(v, types.LambdaType):
                pass
            else:
                self.request[k] = v
        # 设置  field_value_mapping\field_name_2_new_field_name\check_props\identifier
        self._update_props()

        # 执行之后更新已执行标识
        self._prep_request_flag = True

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        loads = json.loads(resp_str)
        loads_response = loads['response']
        if isinstance(loads_response, str):
            loads_response =  json.loads(loads_response)
        elif isinstance(loads_response, dict):
            pass
        if not getattr(self, 'total_page', None):
            setattr(self, 'total_page', int(float(loads_response['data']['totalPage'])))
        return loads_response['data']['list']

    def _row_processor(self, row: dict) -> dict:
        # 处理业绩比较基准
        income_type = row.get('incomeType', '')
        if income_type == '1':
            row[FIELD_MAPPINGS['业绩比较基准']] = json.dumps({
                'title': '预期年化收益率',
                'value': row['productExpectIncomeRate']
            }).encode().decode('unicode_escape')
        else:
            raise CustomException(None, f'出现没有考虑的{income_type}')
        return row

    def _if_end(self, response: Response) -> typing.Optional[bool]:
        """
        在_parse_response之前执行
        :param response:
        :return:
        """
        page_no = getattr(self, 'page_no', None)
        total_page = getattr(self, 'total_page', None)
        current_request_index = getattr(self, 'current_request_index', None)
        total_request_cycle = getattr(self, 'total_request_cycle', None)
        # 至少执行一次请求解析 设置total_page之后 _if_end才能够判断是否达到终止条件
        if total_page is not None:
            if page_no >= total_page:
                # 表示当前请求结束之后需要切换url或终止
                if current_request_index + 1 == total_request_cycle:
                    return True
                else:
                    # 更新所有环境数据变量
                    setattr(self, 'current_request_index', getattr(self, 'current_request_index') + 1)
                    current_iter_request = getattr(self, 'requests_iter')[getattr(self, 'current_request_index')][
                        'request']
                    setattr(self, 'page_no', 1)
                    for k, v in current_iter_request.items():
                        if isinstance(v, types.LambdaType):
                            pass
                        else:
                            self.request[k] = v
                    self._update_props()

    def _row_post_processor(self, row: dict) -> dict:
        row['logId'] = self.log_id
        row['createTime'] = getLocalDate()
        if 'cpbm' not in row.keys():
            row['cpbm'] = row['cpmc']
        row['ywfl'] = getattr(self, 'requests_iter')[getattr(self, 'current_request_index')]['title']
        row['crawl_from'] = 'pc'
        return row

    def log_current_title(self):
        where = 'payh_pc.PayhCrawlRequest.log_current_title'
        current_iter_request = getattr(self, 'requests_iter')[getattr(self, 'current_request_index')]
        log(self.logger, 'info', where, f'正在处理:{current_iter_request["title"]}')

    def _next_request(self):
        if STATE == 'DEV' and getattr(self, 'total_page', None) is not None:
            setattr(self, 'page_no', getattr(self, 'total_page'))
        self.log_current_title()
        if not getattr(self, 'page_no', None):
            setattr(self, 'page_no', 1)
        else:
            setattr(self, 'page_no', getattr(self, 'page_no') + 1)
        self.request['json'] = getattr(self, 'requests_iter')[getattr(self, 'current_request_index')]['request'][
            'json'](
            getattr(self, 'page_no'))


hxyh_crawl_mobile = HxyhCrawlRequest(
    requests_iter=[
        # 华夏银行-爬取产品列表
        {
            'request': {
                'url': 'https://m.hxb.com.cn/wechat/wxbank/finance/finance/netValueFinancial',
                'method': 'post',
                'json': lambda page_no: {
                    "request": {
                        "sortField": "",
                        "sortType": "",
                        "type": "",
                        "productName": "",
                        "page": page_no,
                        "hidderLoading": True
                    }
                },
                'headers': {"Accept-Encoding": "gzip, deflate, br",
                            "Referer": "https://m.hxb.com.cn/wechat/static/index.html?code=041JnAGa100mUD0NPCJa1IAcNr1JnAGX&state=STATE",
                            "Connection": "keep-alive", "Host": "m.hxb.com.cn", "BL": "null",
                            "User-Agent": "Mozilla\/5.0 (iPhone; CPU iPhone OS 15_6_1 like Mac OS X) AppleWebKit\/605.1.15 (KHTML, like Gecko)  Mobile\/15E148 wxwork\/4.0.16 MicroMessenger\/7.0.1 Language\/zh ColorScheme\/Light",
                            "appid": "1606292992775",
                            "Accept-Language": "zh-CN,zh-Hans;q=0.9",
                            "Accept": "application/json, text/plain, */*",
                            "rev": "DGTwR91tAos2LmB/8vZ+J+PINLDBOgiXe7XGl1AzoDN0s8njrrMD53BSA1INZ5o1N/fAAuUBp1y/lGv59ZEGngwF/Wi+HXTUmYLmM09rIbfMRcZDbdwzCbuprGA23sWTRZW5T/OTLeLIl6b14WRuYL8XmHC9qNmhtMzV73xtJG7J3E66hgRg3jz+z3/oSy/jqYhEKKPQfUwoNV5JO34xcO2ic9c2hQdIcGE0TsUTxJWtX6lvBiba095/pzj38kw5XBg2M99iyKoE4EciulK8Pl0UjDq0Bx2r761BJZGOvHTwHeGxfI9v0MzSNE6wlXJC/dAM4tcGbAZc0k3kyCegVQ=="
                            },

            },
            'identifier': 'hxyh',
            'field_value_mapping': {

            },

            'field_name_2_new_field_name': {
                'productLimit': FIELD_MAPPINGS['投资期限'],
                'productCode': FIELD_MAPPINGS['产品编码'],
                'productName': FIELD_MAPPINGS['产品名称'],
            },
            'check_props': ['logId', 'cpbm'],
            'title': '华夏银行-MOBILE'
        },
    ]
)

__all__ = ['hxyh_crawl_mobile']
