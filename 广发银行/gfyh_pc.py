import json
import re
import types
import typing
from typing import List

from bs4 import BeautifulSoup
from requests import Response
from utils.crawl_request import AbstractCrawlRequest
from utils.db_utils import getLocalDate
from utils.html_utils import parse_table
from utils.logging_utils import log
from utils.mappings import FIELD_MAPPINGS
from gfyh_config import STATE


class GfyhCrawlRequest(AbstractCrawlRequest):

    def _update_props(self):
        index = getattr(self, 'current_request_index')
        self.field_name_2_new_field_name = getattr(self, 'requests_iter')[index]['field_name_2_new_field_name']
        self.field_value_mapping = getattr(self, 'requests_iter')[index]['field_value_mapping']
        self.check_props = getattr(self, 'requests_iter')[index]['check_props']
        self.identifier = getattr(self, 'requests_iter')[index]['identifier']

    def _prep_request(self):
        self.request = {}
        self.go_on = True
        # 仅执行一次
        # 初始化状态：当前执行第一个请求\page_no = None\total_request_cycle
        setattr(self, 'requests_iter', self.kwargs['requests_iter'])
        setattr(self, 'current_request_index', 0)
        setattr(self, 'total_request_cycle', len(self.kwargs['requests_iter']))
        setattr(self, 'page_no', 1)
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
        resp_str = response.content.decode(response.encoding) \
            if response.encoding \
            else response.text
        if not getattr(self, 'total_page', None):
            # 没有设置总页数
            findall = re.findall(r'第\d/(\d+)页', resp_str)
            setattr(self, 'total_page', int(findall[0]))

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

    def _row_processor(self, row: dict) -> dict:

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
                    # 需要切换url
                    setattr(self, 'current_request_index', getattr(self, 'current_request_index') + 1)
                    current_iter_request = getattr(self, 'requests_iter')[getattr(self, 'current_request_index')][
                        'request']
                    setattr(self, 'page_no', None)
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
        where = 'gfyh_pc2.GfyhCrawlRequest.log_current_title'
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


#


gfyh_crawl_pc = GfyhCrawlRequest(
    requests_iter=[
        # 代销资管产品
        {
            'request': {
                'url': 'http://www.cgbchina.com.cn/Channel/22598208',
                'method': 'post',
                'json': lambda page_no: {
                    'proName': '',
                    'proCode': '',
                    'curPage': f'{page_no}'
                },
                'headers': {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                    "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Cache-Control": "no-cache",
                    "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                    "Proxy-Connection": "keep-alive",
                    "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                },

            },
            'identifier': 'gfyh',
            'field_value_mapping': {},
            'field_name_2_new_field_name': {
                '产品名称': FIELD_MAPPINGS['产品名称'],
                '产品代码': FIELD_MAPPINGS['产品编码'],
                '产品来源': FIELD_MAPPINGS['管理人'],
                '风险等级': FIELD_MAPPINGS['风险等级'],
                '起点金额（元）': FIELD_MAPPINGS['起购金额'],
                '产品期限': FIELD_MAPPINGS['投资期限']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '代销资管产品'
        },
        # 保险
        {
            'request': {
                'url': 'http://www.cgbchina.com.cn/Channel/22598600',
                'method': 'post',
                'json': lambda page_no: {
                    'proName': '',
                    'proCode': '',
                    'curPage': f'{page_no}'
                },
                'headers': {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                    "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Cache-Control": "no-cache",
                    "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                    "Proxy-Connection": "keep-alive",
                    "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                },

            },
            'identifier': 'gfyh',
            'field_value_mapping': {},
            'field_name_2_new_field_name': {
                '类型': FIELD_MAPPINGS['投资性质'],
                '产品名称': FIELD_MAPPINGS['产品名称'],
                '产品代码': FIELD_MAPPINGS['产品编码'],
                '风险等级': FIELD_MAPPINGS['风险等级'],
                '起点金额（元）': FIELD_MAPPINGS['起购金额'],
            },
            'check_props': ['logId', 'cpbm'],
            'title': '保险'
        },
        # 公募基金
        {
            'request': {
                'url': 'http://www.cgbchina.com.cn/Channel/22598502',
                'method': 'post',
                'json': lambda page_no: {
                    'proName': '',
                    'proCode': '',
                    'curPage': f'{page_no}'
                },
                'headers': {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                    "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Cache-Control": "no-cache",
                    "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                    "Proxy-Connection": "keep-alive",
                    "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                },

            },
            'identifier': 'gfyh',
            'field_value_mapping': {},
            'field_name_2_new_field_name': {
                '产品代码': FIELD_MAPPINGS['产品编码'],
                '产品名称': FIELD_MAPPINGS['产品名称'],
                '产品类型': FIELD_MAPPINGS['募集方式'],
                '产品管理人': FIELD_MAPPINGS['管理人'],
                '销售区域': FIELD_MAPPINGS['销售区域'],
                '风险等级': FIELD_MAPPINGS['风险等级'],
                '产品状态': FIELD_MAPPINGS['产品状态'],
                '产品净值': FIELD_MAPPINGS['净值']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '公募基金'
        },
        # 理财产品
        {
            'request': {
                'url': 'http://www.cgbchina.com.cn/Channel/22598110',
                'method': 'post',
                'json': lambda page_no: {
                    'proName': '',
                    'proCode': '',
                    'curPage': f'{page_no}'
                },
                'headers': {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                    "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Cache-Control": "no-cache",
                    "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                    "Proxy-Connection": "keep-alive",
                    "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                },

            },
            'identifier': 'gfyh',
            'field_value_mapping': {},
            'field_name_2_new_field_name': {
                '类型': FIELD_MAPPINGS['募集方式'],
                '产品名称': FIELD_MAPPINGS['产品名称'],
                '产品代码': FIELD_MAPPINGS['产品编码'],
                '发行机构': FIELD_MAPPINGS['管理人'],
                '风险等级': FIELD_MAPPINGS['风险等级'],
                '起点金额（元）': FIELD_MAPPINGS['起购金额'],
                '产品期限': FIELD_MAPPINGS['投资期限'],
            },
            'check_props': ['logId', 'cpbm'],
            'title': '理财产品'
        },
        # 私募专户
        {
            'request': {
                'url': 'http://www.cgbchina.com.cn/Channel/22598404',
                'method': 'post',
                'json': lambda page_no: {
                    'proName': '',
                    'proCode': '',
                    'curPage': f'{page_no}'
                },
                'headers': {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                    "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Cache-Control": "no-cache",
                    "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                    "Proxy-Connection": "keep-alive",
                    "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                },

            },
            'identifier': 'gfyh',
            'field_value_mapping': {},
            'field_name_2_new_field_name': {
                '类型': FIELD_MAPPINGS['募集方式'],
                '产品名称': FIELD_MAPPINGS['产品名称'],
                '产品代码': FIELD_MAPPINGS['产品编码'],
                '产品来源': FIELD_MAPPINGS['管理人'],
                '风险等级': FIELD_MAPPINGS['风险等级'],
                '起点金额（元）': FIELD_MAPPINGS['起购金额'],
                '持有期限（天）': FIELD_MAPPINGS['投资期限']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '私募专户'
        },
        # 结构性存款
        {
            'request': {
                'url': 'http://www.cgbchina.com.cn/Channel/24530070',
                'method': 'post',
                'json': lambda page_no: {
                    'proName': '',
                    'proCode': '',
                    'curPage': f'{page_no}'
                },
                'headers': {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                    "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Cache-Control": "no-cache",
                    "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                    "Proxy-Connection": "keep-alive",
                    "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                },

            },
            'identifier': 'gfyh',
            'field_value_mapping': {
                '起点金额（元）': lambda x: str(x) + '元' if not x.endswith('元') else x,

            },
            'field_name_2_new_field_name': {
                '类型': FIELD_MAPPINGS['投资性质'],
                '产品名称': FIELD_MAPPINGS['产品名称'],
                '产品代码': FIELD_MAPPINGS['产品编码'],
                '风险等级': FIELD_MAPPINGS['风险等级'],
                '起点金额（元）': FIELD_MAPPINGS['起购金额'],
                '产品期限': FIELD_MAPPINGS['投资期限']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '结构性存款'
        },
        # 贵金属
        {
            'request': {
                'url': 'http://www.cgbchina.com.cn/Channel/22598698',
                'method': 'post',
                'json': lambda page_no: {
                    'proName': '',
                    'proCode': '',
                    'curPage': f'{page_no}'
                },
                'headers': {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                    "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Cache-Control": "no-cache",
                    "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                    "Proxy-Connection": "keep-alive",
                    "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                },

            },
            'identifier': 'gfyh',
            'field_value_mapping': {
                '起点金额（元）': lambda x: str(x) + '元' if not x.endswith('元') else x,

            },
            'field_name_2_new_field_name': {
                '类型': FIELD_MAPPINGS['投资性质'],
                '产品名称': FIELD_MAPPINGS['产品名称'],
                '风险等级': FIELD_MAPPINGS['风险等级'],
                '起点金额（元）': FIELD_MAPPINGS['起购金额'],
            },
            'check_props': ['logId', 'cpbm'],
            'title': '贵金属'
        },
        # 信托产品
        {
            'request': {
                'url': 'http://www.cgbchina.com.cn/Channel/22598306',
                'method': 'post',
                'json': lambda page_no: {
                    'proName': '',
                    'proCode': '',
                    'curPage': f'{page_no}'
                },
                'headers': {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                    "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Cache-Control": "no-cache",
                    "Host": "www.cgbchina.com.cn", "Pragma": "no-cache",
                    "Proxy-Connection": "keep-alive",
                    "Referer": "http//www.cgbchina.com.cn/Channel/25518271",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                },

            },
            'identifier': 'gfyh',
            'field_value_mapping': {
                '起点金额（元）': lambda x: str(x) + '元' if not x.endswith('元') else x,

            },
            'field_name_2_new_field_name': {
                '类型': FIELD_MAPPINGS['募集方式'],
                '产品名称': FIELD_MAPPINGS['产品名称'],
                '产品代码': FIELD_MAPPINGS['产品编码'],
                '产品来源': FIELD_MAPPINGS['管理人'],
                '风险等级': FIELD_MAPPINGS['风险等级'],
                '起点金额（元）': FIELD_MAPPINGS['起购金额'],
                '持有期限（天）': FIELD_MAPPINGS['投资期限']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '信托产品'
        },

    ]
)

__all__ = ['gfyh_crawl_pc']
