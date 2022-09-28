import json
import math
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
from payh_config import STATE

####################
# https://ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true
####################
from utils.string_utils import remove_space


class PayhCrawlRequest(AbstractCrawlRequest):

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
        resp_str = response.text.encode(response.encoding).decode('utf-8', errors='ignore') \
            if response.encoding else response.text
        loads = json.loads(resp_str)
        rows = loads['data']['superviseProductInfoList']
        if not getattr(self, 'total_page', None):
            setattr(self, 'total_page', math.ceil(int(float(loads['data']['totalSize'])) / len(rows)))
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
        self.request['data'] = getattr(self, 'requests_iter')[getattr(self, 'current_request_index')]['request'][
            'data'](
            getattr(self, 'page_no'))


#


payh_crawl_pc = PayhCrawlRequest(
    requests_iter=[
        # 代销产品-保险
        {
            'request': {
                'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
                'method': 'post',
                'data': lambda page_no: {
                    'tableIndex': 'table08',
                    'dataType': '08',
                    'tplId': 'tpl08',
                    'pageNum': f'{page_no}',
                    'pageSize': 20,
                    'channelCode': 'C0002',
                    'access_source': 'PC',
                },
                'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                            "accept-language": "zh-CN,zh;q=0.9",
                            "content-length": "99",
                            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                            "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                            "origin": "https//ebank.pingan.com.cn",
                            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                            },

            },
            'identifier': 'payh',
            'field_value_mapping': {
                'minAmount': lambda x: str(x) + '元',
            },
            'field_name_2_new_field_name': {
                'investmentScope': FIELD_MAPPINGS['投资性质'],
                'riskLevel': FIELD_MAPPINGS['风险等级'],
                'prdName': FIELD_MAPPINGS['产品名称'],
                'prdManager': FIELD_MAPPINGS['管理人'],
                'prdCode': FIELD_MAPPINGS['产品编码'],
                'status': FIELD_MAPPINGS['产品状态']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '代销产品-保险'
        },
        # 代销产品-保险金信托
        {
            'request': {
                'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
                'method': 'post',
                'data': lambda page_no: {
                    'tableIndex': 'table09',
                    'dataType': '09',
                    'tplId': 'tpl09',
                    'pageNum': f'{page_no}',
                    'pageSize': 20,
                    'channelCode': 'C0002',
                    'access_source': 'PC',
                },
                'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                            "accept-language": "zh-CN,zh;q=0.9",
                            "content-length": "99",
                            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                            "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                            "origin": "https//ebank.pingan.com.cn",
                            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                            },

            },
            'identifier': 'payh',
            'field_value_mapping': {
                'minAmount': lambda x: str(x) + '元' if remove_space(str(x)) else '',
            },
            'field_name_2_new_field_name': {
                'riskLevel': FIELD_MAPPINGS['风险等级'],
                'productName': FIELD_MAPPINGS['产品名称'],
                'productType': FIELD_MAPPINGS['投资性质'],
                'issuer': FIELD_MAPPINGS['管理人']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '代销产品-保险金信托'
        },
        # 本行产品-对公结构性存款
        {
            'request': {
                'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
                'method': 'post',
                'data': lambda page_no: {
                    'tableIndex': 'table10',
                    'dataType': '10',
                    'tplId': 'tpl10',
                    'pageNum': f'{page_no}',
                    'pageSize': 20,
                    'channelCode': 'C0002',
                    'access_source': 'PC',
                },
                'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                            "accept-language": "zh-CN,zh;q=0.9",
                            "content-length": "99",
                            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                            "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                            "origin": "https//ebank.pingan.com.cn",
                            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                            },

            },
            'identifier': 'payh',
            'field_value_mapping': {
                'minAmount': lambda x: str(x) + '元',
                'rateType': {
                    'BF': '保本浮动收益'
                },
                'riskLevel': {
                    '1': '低等风险',
                    '2': '中低风险'
                },
                'saleStatus': {
                    '1': '在售',
                    '2': '存续'
                },
            },
            'field_name_2_new_field_name': {
                'minAmount': FIELD_MAPPINGS['起购金额'],
                'riskLevel': FIELD_MAPPINGS['风险等级'],
                'prdName': FIELD_MAPPINGS['产品名称'],
                'manageBankName': FIELD_MAPPINGS['管理人'],
                'saleStatus': FIELD_MAPPINGS['产品状态'],
                'prdCode': FIELD_MAPPINGS['产品编码'],
                'rateType': FIELD_MAPPINGS['收益类型'],
                'taCode': FIELD_MAPPINGS['TA编码'],
                'tranBankSet': FIELD_MAPPINGS['销售渠道'],
                'productNo': FIELD_MAPPINGS['登记编码']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '本行产品-对公结构性存款'
        },
        # 本行产品-公募基金
        {
            'request': {
                'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
                'method': 'post',
                'data': lambda page_no: {
                    'tableIndex': 'table10',
                    'dataType': '10',
                    'tplId': 'tpl10',
                    'pageNum': f'{page_no}',
                    'pageSize': 20,
                    'channelCode': 'C0002',
                    'access_source': 'PC',
                },
                'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                            "accept-language": "zh-CN,zh;q=0.9",
                            "content-length": "99",
                            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                            "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                            "origin": "https//ebank.pingan.com.cn",
                            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                            },

            },
            'identifier': 'payh',
            'field_value_mapping': {
                'minAmount': lambda x: str(x) + '元',
                'rateType': {
                    'BF': '保本浮动收益'
                },
                'riskLevel': {
                    '1': '低等风险',
                    '2': '中低风险',
                    '4': '中高风险'
                },
                'saleStatus': {
                    '1': '在售',
                    '2': '存续'
                },
            },
            'field_name_2_new_field_name': {
                'riskLevel': FIELD_MAPPINGS['风险等级'],
                'prdName': FIELD_MAPPINGS['产品名称'],
                'prdCode': FIELD_MAPPINGS['产品编码'],
                'prdArrName': FIELD_MAPPINGS['投资性质'],
                'taCode': FIELD_MAPPINGS['TA编码'],
                'taName': FIELD_MAPPINGS['TA名称'],
                'prdManager': FIELD_MAPPINGS['管理人'],
                'status': FIELD_MAPPINGS['产品状态']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '本行产品-公募基金'
        },
        # 代销产品-信托/资管计划及其他
        {
            'request': {
                'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
                'method': 'post',
                'data': lambda page_no: {
                    'tableIndex': 'table04',
                    'dataType': '04',
                    'tplId': 'tpl03',
                    'pageNum': f'{page_no}',
                    'pageSize': 20,
                    'channelCode': 'C0002',
                    'access_source': 'PC',
                },
                'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                            "accept-language": "zh-CN,zh;q=0.9",
                            "content-length": "99",
                            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                            "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                            "origin": "https//ebank.pingan.com.cn",
                            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                            },

            },
            'identifier': 'payh',
            'field_value_mapping': {
                'minAmount': lambda x: str(x) + '元' if remove_space(str(x)) else '',
                'riskLevel': {
                    '1': '低等风险',
                    '2': '中低风险',
                    '4': '中高风险'
                },
            },
            'field_name_2_new_field_name': {
                'riskLevel': FIELD_MAPPINGS['风险等级'],
                'taCode': FIELD_MAPPINGS['TA编码'],
                'prdName': FIELD_MAPPINGS['产品名称'],
                'taName': FIELD_MAPPINGS['TA名称'],
                'prdManager': FIELD_MAPPINGS['管理人'],
                'prdCode': FIELD_MAPPINGS['产品编码']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '代销产品-信托/资管计划及其他'
        },
        # 代销产品-理财子
        {
            'request': {
                'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
                'method': 'post',
                'data': lambda page_no: {
                    'tableIndex': 'table02',
                    'dataType': '02',
                    'tplId': 'tpl02',
                    'pageNum': f'{page_no}',
                    'pageSize': 20,
                    'channelCode': 'C0002',
                    'access_source': 'PC',
                },
                'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                            "accept-language": "zh-CN,zh;q=0.9",
                            "content-length": "99",
                            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                            "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                            "origin": "https//ebank.pingan.com.cn",
                            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                            },

            },
            'identifier': 'payh',
            'field_value_mapping': {
                'minAmount': lambda x: str(x) + '元' if remove_space(str(x)) else '',
                'riskLevel': {
                    '1': '低等风险',
                    '2': '中低风险',
                    '4': '中高风险'
                },
            },
            'field_name_2_new_field_name': {
                'riskLevel': FIELD_MAPPINGS['风险等级'],
                'prdName': FIELD_MAPPINGS['产品名称'],
                'saleStatus': FIELD_MAPPINGS['产品状态'],
                'prdCode': FIELD_MAPPINGS['产品编码'],
                'taCode': FIELD_MAPPINGS['TA编码'],
                'prdManager': FIELD_MAPPINGS['管理人'],
            },
            'check_props': ['logId', 'cpbm'],
            'title': '代销产品-理财子'
        },
        # 本行产品-个人结构性存款
        {
            'request': {
                'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
                'method': 'post',
                'data': lambda page_no: {
                    'tableIndex': 'table06',
                    'dataType': '06',
                    'tplId': 'tpl06',
                    'pageNum': f'{page_no}',
                    'pageSize': 20,
                    'channelCode': 'C0002',
                    'access_source': 'PC',
                },
                'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                            "accept-language": "zh-CN,zh;q=0.9",
                            "content-length": "99",
                            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                            "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                            "origin": "https//ebank.pingan.com.cn",
                            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                            },

            },
            'identifier': 'payh',
            'field_value_mapping': {
                'rateType': {
                    'BF': '保本浮动收益'
                },
                'riskLevel': {
                    '2': '中低风险',
                },
                'saleStatus': {
                    '1': '存续',
                    '0': '在售'
                },
            },
            'field_name_2_new_field_name': {
                'minAmount': FIELD_MAPPINGS['起购金额'],
                'rateType': FIELD_MAPPINGS['收益类型'],
                'riskLevel': FIELD_MAPPINGS['风险等级'],
                'prdName': FIELD_MAPPINGS['产品名称'],
                'saleStatus': FIELD_MAPPINGS['产品状态'],
                'prdCode': FIELD_MAPPINGS['产品编码'],
            },
            'check_props': ['logId', 'cpbm'],
            'title': '本行产品-个人结构性存款'
        },
        # 代销产品-养老保险
        {
            'request': {
                'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
                'method': 'post',
                'data': lambda page_no: {
                    'tableIndex': 'table03',
                    'dataType': '03',
                    'tplId': 'tpl02',
                    'pageNum': f'{page_no}',
                    'pageSize': 20,
                    'channelCode': 'C0002',
                    'access_source': 'PC',
                },
                'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                            "accept-language": "zh-CN,zh;q=0.9",
                            "content-length": "99",
                            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                            "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                            "origin": "https//ebank.pingan.com.cn",
                            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                            },

            },
            'identifier': 'payh',
            'field_value_mapping': {
                'status': {
                    '5': '停止申购'
                },
                'riskLevel': {
                    '3': '中等风险'
                }
            },
            'field_name_2_new_field_name': {
                'riskLevel': FIELD_MAPPINGS['风险等级'],
                'prdName': FIELD_MAPPINGS['产品名称'],
                'prdCode': FIELD_MAPPINGS['产品编码'],
                'taCode': FIELD_MAPPINGS['TA编码'],
                'taName': FIELD_MAPPINGS['TA名称'],
                'prdManager': FIELD_MAPPINGS['管理人'],
                'status': FIELD_MAPPINGS['产品状态']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '代销产品-养老保险'
        },
        # 本行产品-银行理财
        {
            'request': {
                'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
                'method': 'post',
                'data': lambda page_no: {
                    'tableIndex': 'table01',
                    'dataType': '01',
                    'tplId': 'tpl01',
                    'pageNum': f'{page_no}',
                    'pageSize': 20,
                    'channelCode': 'C0002',
                    'access_source': 'PC',
                },
                'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                            "accept-language": "zh-CN,zh;q=0.9",
                            "content-length": "99",
                            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                            "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                            "origin": "https//ebank.pingan.com.cn",
                            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                            },

            },
            'identifier': 'payh',
            'field_value_mapping': {
                'rateType': {
                    'FF': '非保本浮动收益',
                },
                'riskLevel': {
                    '2': '中低风险',
                    '3': '中等风险',

                },
                'saleStatus': {
                    '0': '在售',
                    '1': '存续'
                },
            },
            'field_name_2_new_field_name': {
                'minAmount': FIELD_MAPPINGS['起购金额'],
                'riskLevel': FIELD_MAPPINGS['风险等级'],
                'prdName': FIELD_MAPPINGS['产品名称'],
                'saleStatus': FIELD_MAPPINGS['产品状态'],
                'prdCode': FIELD_MAPPINGS['产品编码'],
                'productNo': FIELD_MAPPINGS['登记编码'],
                'rateType': FIELD_MAPPINGS['收益类型']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '本行产品-银行理财'
        },
        # 本行产品-银行公司理财
        {
            'request': {
                'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
                'method': 'post',
                'data': lambda page_no: {
                    'tableIndex': 'table05',
                    'dataType': '05',
                    'tplId': 'tpl03',
                    'pageNum': f'{page_no}',
                    'pageSize': 20,
                    'channelCode': 'C0002',
                    'access_source': 'PC',
                },
                'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                            "accept-language": "zh-CN,zh;q=0.9",
                            "content-length": "99",
                            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                            "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                            "origin": "https//ebank.pingan.com.cn",
                            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                            },

            },
            'identifier': 'payh',
            'field_value_mapping': {
                'minAmount': lambda x: str(x) + '元',
            },
            'field_name_2_new_field_name': {
                'minAmount': FIELD_MAPPINGS['起购金额'],
                'riskLevel': FIELD_MAPPINGS['风险等级'],
                'prdName': FIELD_MAPPINGS['产品名称'],
                'manageBankName': FIELD_MAPPINGS['管理人'],
                'saleStatus': FIELD_MAPPINGS['产品状态'],
                'prdCode': FIELD_MAPPINGS['产品编码'],
                'rateType': FIELD_MAPPINGS['收益类型'],
                'taCode': FIELD_MAPPINGS['TA编码'],
                'tranBankSet': FIELD_MAPPINGS['销售渠道'],
                'productNo': FIELD_MAPPINGS['登记编码'],
            },
            'check_props': ['logId', 'cpbm'],
            'title': '本行产品-银行公司理财'
        },
        # 代销产品-公司代销理财
        {
            'request': {
                'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
                'method': 'post',
                'data': lambda page_no: {
                    'tableIndex': 'table11',
                    'dataType': '11',
                    'tplId': 'tpl11',
                    'pageNum': f'{page_no}',
                    'pageSize': 20,
                    'channelCode': 'C0002',
                    'access_source': 'PC',
                },
                'headers': {"accept": "*/*", "accept-encoding": "gzip, deflate, br",
                            "accept-language": "zh-CN,zh;q=0.9",
                            "content-length": "99",
                            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                            "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
                            "origin": "https//ebank.pingan.com.cn",
                            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"",
                            "sec-fetch-dest": "empty",
                            "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
                            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
                            },

            },
            'identifier': 'payh',
            'field_value_mapping': {
                'minAmount': lambda x: str(x) + '元' if remove_space(str(x)) else '',
            },
            'field_name_2_new_field_name': {
                'minAmount': FIELD_MAPPINGS['起购金额'],
                'riskLevel': FIELD_MAPPINGS['风险等级'],
                'prdName': FIELD_MAPPINGS['产品名称'],
                'manageBankName': FIELD_MAPPINGS['管理人'],
                'saleStatus': FIELD_MAPPINGS['产品状态'],
                'prdCode': FIELD_MAPPINGS['产品编码'],
                'rateType': FIELD_MAPPINGS['收益类型'],
                'taCode': FIELD_MAPPINGS['TA编码'],
                'tranBankSet': FIELD_MAPPINGS['销售渠道'],
                'productNo': FIELD_MAPPINGS['登记编码']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '代销产品-公司代销理财'
        },
    ]
)

__all__ = ['payh_crawl_pc']
