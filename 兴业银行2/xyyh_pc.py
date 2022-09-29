import json
import math
import re
import time
import types
import typing
from typing import List

from bs4 import BeautifulSoup
from requests import Response

from utils.common_utils import delete_empty_value
from utils.crawl_request import AbstractCrawlRequest
from utils.custom_exception import cast_exception
from utils.db_utils import getLocalDate
from utils.html_utils import parse_table
from utils.logging_utils import log
from utils.mappings import FIELD_MAPPINGS
from xyyh_config import STATE, SLEEP_SECOND


class XyyhCrawlRequest(AbstractCrawlRequest):
    def _update_props(self):
        index = getattr(self, 'current_request_index')
        field_name_2_new_field_name = getattr(self, 'requests_iter')[index].get('field_name_2_new_field_name', None)
        default_field_name_2_new_field_name = self.kwargs.get('default_field_name_2_new_field_name', None)
        setattr(self, 'field_name_2_new_field_name',
                field_name_2_new_field_name if field_name_2_new_field_name else default_field_name_2_new_field_name)

        field_value_mapping = getattr(self, 'requests_iter')[index].get('field_value_mapping')
        default_field_value_mapping = self.kwargs.get('default_field_value_mapping', None)
        setattr(self, 'field_value_mapping',
                field_value_mapping if field_value_mapping else default_field_value_mapping)

        check_props = getattr(self, 'requests_iter')[index].get('check_props', None)
        default_check_props = self.kwargs.get('default_check_props', None)
        setattr(self, 'check_props', check_props if check_props else default_check_props)

        identifier = getattr(self, 'requests_iter')[index].get('identifier', None)
        default_identifier = self.kwargs.get('default_identifier', None)
        setattr(self, 'identifier', identifier if identifier else default_identifier)

        headers = getattr(self, 'requests_iter')[index].get('headers', None)
        default_headers = self.kwargs.get('default_headers', None)
        setattr(self, 'headers', headers if headers else default_headers)

        method = getattr(self, 'requests_iter')[index].get('method', None)
        default_method = self.kwargs.get('default_method', None)
        setattr(self, 'method', method if method else default_method)

    def _transform_row(self, rows):
        processed_rows = []
        for row in rows:
            mjfs_list = [row.get('募集方式', None), row.get('募集方式（公募/私募）', None)]
            mjfs = None
            for item in mjfs_list:
                if item:
                    mjfs = item
                    break
            fxdj_list = [row.get('代销风险等级', None), row.get('产品风险等级', None), row.get('产品风险级别', None)]
            fxdj = None
            for item in fxdj_list:
                if item:
                    fxdj = item
                    break
            cpbm_list = [row.get('产品代码', None), row.get('产品代号', None)]
            cpbm = None
            for item in cpbm_list:
                if item:
                    cpbm = item
                    break
            processed_row = {
                FIELD_MAPPINGS['募集方式']: mjfs,
                FIELD_MAPPINGS['产品名称']: row.get('产品名称', None),
                FIELD_MAPPINGS['登记编码']: row.get('银行业理财信息登记系统登记编码', None),
                FIELD_MAPPINGS['产品编码']: cpbm,
                FIELD_MAPPINGS['产品简称']: row.get('产品简称', None),
                FIELD_MAPPINGS['风险等级']: fxdj,
                FIELD_MAPPINGS['管理人']: row.get('发行机构', None),
                FIELD_MAPPINGS['投资性质']: row.get('产品类型', None),
                FIELD_MAPPINGS['起息日']: row.get('产品起息日', None),
                FIELD_MAPPINGS['预计到期利率']: row.get('预计到期利率（%）', None),
                FIELD_MAPPINGS['产品状态']: row.get('产品状态', None),

            }
            delete_empty_value(processed_row)
            processed_rows.append(processed_row)
        return processed_rows

    def _prep_request(self):
        # 初始化爬虫状态
        self.request = {}
        setattr(self, 'requests_iter', self.kwargs['requests_iter'])
        setattr(self, 'current_request_index', 0)
        setattr(self, 'total_request_cycle', len(self.kwargs['requests_iter']))
        # 初始化页码和总页数
        setattr(self, 'page_no', 1)
        setattr(self, 'total_page', 1)
        self._update_props()
        current_iter_request = getattr(self, 'requests_iter')[0]
        for k, v in current_iter_request['request'].items():
            if isinstance(v, types.LambdaType):
                pass
            else:
                self.request[k] = v if v else getattr(self, k)
        # 执行之后更新已执行标识
        self._prep_request_flag = True

    def _do_parse(self, resp_str, head_tag):
        try:
            soup = BeautifulSoup(resp_str, 'lxml')
            table_tags = soup.select('table')
            if table_tags:
                table_tag = table_tags[0]
                rows = parse_table(
                    table=table_tag,
                    col_names=None,
                    callbacks={},
                    extra_attrs={},
                    head_tag=head_tag
                )
                processed_rows = self._transform_row(rows)
                return processed_rows
            return []
        except Exception as e:
            exception = cast_exception(e)
            raise exception

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        rows = self._do_parse(resp_str, 'td')
        return rows

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
                    # 更新所有环境数据变量
                    setattr(self, 'current_request_index', getattr(self, 'current_request_index') + 1)
                    current_iter_request = getattr(self, 'requests_iter')[getattr(self, 'current_request_index')][
                        'request']
                    # 每个url只请求一次 故设置page_no  = total_page
                    setattr(self, 'page_no', 1)
                    setattr(self, 'total_page', 1)
                    self._update_props()
                    for k, v in current_iter_request.items():
                        if isinstance(v, types.LambdaType):
                            pass
                        else:
                            self.request[k] = v if v else getattr(self, k)

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


xyyh_crawl_pc = XyyhCrawlRequest(
    requests_iter=[
        # 本行理财产品
        {
            'request': {
                'url': 'https://www.cib.com.cn/cn/personal/wealth-management/xxcx/table/',
                'headers': '',
                'method': ''
            },
            'title': '本行理财产品'
        },
        # 其他理财产品（代销）
        {
            'request': {
                'url': 'https://www.cib.com.cn/cn/personal/wealth-management/xxcx/dxfund.html',
                'headers': '',
                'method': ''
            },
            'title': '其他理财产品（代销）'
        },
        # 理财产品（代销）
        {
            'request': {
                'url': 'https://www.cib.com.cn/cn/personal/wealth-management/xxcx/pfund.html',
                'headers': '',
                'method': ''
            },
            'title': '理财产品（代销）'
        },
        # 基金（代销）
        {
            'request': {
                'url': 'https://www.cib.com.cn/cn/personal/wealth-management/xxcx/fundsp.html',
                'headers': '',
                'method': ''
            },
            'title': '基金（代销）'
        },
        # 券商集合（代销）
        {
            'request': {
                'url': 'https://www.cib.com.cn/cn/personal/wealth-management/xxcx/brokerset.html',
                'headers': '',
                'method': ''
            },
            'title': '券商集合（代销）'
        },
        # 结构性存款
        {
            'request': {
                'url': 'https://www.cib.com.cn/cn/personal/wealth-management/xxcx/JGXCK.html',
                'headers': '',
                'method': ''
            },
            'title': '结构性存款'
        },
        # 信托
        {
            'request': {
                'url': 'https://www.cib.com.cn/cn/personal/wealth-management/xxcx/trusts.html',
                'headers': '',
                'method': ''
            },
            'title': '信托'
        },
        # 实物贵金属（代理）
        {
            'request': {
                'url': 'https://www.cib.com.cn/cn/personal/wealth-management/xxcx/gjs.html',
                'headers': '',
                'method': ''
            },
            'title': '实物贵金属（代理）'
        },
        # 保险（代销）
        {
            'request': {
                'url': 'https://www.cib.com.cn/cn/personal/wealth-management/xxcx/insurance.html',
                'headers': '',
                'method': ''
            },
            'title': '保险（代销）'
        },
    ],
    default_headers={
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": "www.cib.com.cn",
        "Pragma": "no-cache",
        "Referer": "https//www.cib.com.cn/cn/personal/wealth-management/xxcx/pfund.html",
        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": "\"Android\"",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36"
    },
    default_method='get',
    default_identifier='xyyh',
    default_check_props=['logId', 'cpbm']
)

__all__ = ['xyyh_crawl_pc']
