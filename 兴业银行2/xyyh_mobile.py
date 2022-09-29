import json
import math
import re
import time
import types
import typing
from typing import List

import requests
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

    def __init_authentication(self):
        response = self.session.request(
            url='https://g.cib.com.cn/mobile/product/index ',
            headers={"Host": "g.cib.com.cn", "Connection": "keep-alive", "Cache-Control": "max-age=0",
                     "Upgrade-Insecure-Requests": "1",
                     "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x6700143B) WindowsWechat(0x6307062c)",
                     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                     "Cookie": "sajssdk_2015_cross_new_user=1; BIGipServerjtmh_7029_pool=!BzZ8PrxbyYd1A1+MTIck2CZx5lW4VooBg6ElizlAHOnUXxyTpDPGTGCUvLBR0SdpHMzbp4gnz+YuSy8=; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%221838847329774-0428987dab7fc8-61256b25-2073600-18388473298949%22%2C%22%24device_id%22%3A%221838847329774-0428987dab7fc8-61256b25-2073600-18388473298949%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%7D%7D; BSFIT_EXPIRATION=1667097259286; BSFIT_DEVICEID=fGSSG18Ba1nQfaCK3a3HS_KaTHVskTM9yNS99vg0qzniSxU0iasKXj3EQTDV_jyvgSPHPQYup_VEfa4Xy9SkFCW9iSlYYhEDywpwWRQE3bv04kSXkOAk-FlXI_E_BhGXIxYEWxGbVWDJViGYGOvCFyf3b0oiMgyx",
                     "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-User": "?1",
                     "Sec-Fetch-Dest": "document", "Accept-Encoding": "gzip, deflate, br",
                     "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7"}
            ,
            method='get')
        cookies = requests.utils.dict_from_cookiejar(self.session.cookies)
        setattr(self, 'cookies', cookies)
        response = self.session.request(
            url='https://www.zhihu.com/api/v3/account/api/login/qrcode/YWM3YWIxZTgtYmUz/scan_info',
            headers={
                "Host": "www.zhihu.com", "Connection": "keep-alive", "x-zse-93": "101_3_3.0", "x-ab-param": "",
                "x-ab-pb": "CsgBGwA/AEcAtABpAWoBdAE7AswC1wLYAk8DUAOgA6EDogO3A/MD9AMzBIwEjQSmBNYEEQUyBVEFiwWMBZ4FMAYxBn4G6wYnB3cHeAfYB9wH3QdnCHQIdgh5CMUI2gg/CUIJYAmNCcMJxAnFCcYJxwnICckJygnLCcwJ0QnxCfQJBApJCmUKawqYCqUKqQq+CsQK1ArdCu0K/Qr+CjsLPAtDC0YLcQt2C4ULhwuNC8AL1wvgC+UL5gssDDgMcQyPDKwMuQzDDMkM+AwSZAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                "x-requested-with": "fetch",
                "x-zse-96": "2.0_B2rH/jsGQNYNYhVk0GbkTH2YRmU11pr3Ndty0BXU3f8g6IrOymNdqszzorzsLrFq",
                "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x6700143B) WindowsWechat(0x6307062c)",
                "x-zst-81": "3_2.0VhnTj77m-qofgh3TxTnq2_Qq2LYuDhV80wSL7T9yr6nxeTtqXRFZSTrZgcO9kUS_IHkmWgLBpukYWgxmIQPYqbpfZUnPS0FZG0Yq-6C1ZhSYVCxm1uV_70RqSMCBVuky16P0riRCyufOovxBXgSCiiR0ELYuUrxmtDomqU7ynXtOnAoTh_PhRDSTF4HBJ9HLsBOYTwOLNq2Kbu2YFwgqzuxKVCFLpcHKHCF1EJCO6GLfQUH9fXeTvHw12reBkMCfKic1hJVZSG3me9V_XJHGgg31bwXKoXeMt9F16AXCUDcubgxfjDLKxD9mArSshgpOzUt9YU3qLucXzwNMtgpCoqcBQrNMn9Ym3Cg_YCXmVqkwuhcMggH0Vbx0Irx89vXG_UCyeJxYS8pfVDOKN9NMPbC1hUOGz9L8Qqc_DgxYIgVBeT21uCwqovof29LKbAXCUC2YUcLGDqgLbwgMp9XYeMeBWhHM2rSC",
                "Accept": "*/*", "Origin": "https//zhuanlan.zhihu.com", "Sec-Fetch-Site": "same-site",
                "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty",
                "Referer": "https//zhuanlan.zhihu.com/p/27608348", "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9",
                "Cookie": "_zap=c327c9ce-9c84-4329-bac7-79fd9b475c76; d_c0=\"APDQlw0SHRKPTqZqOrsE3dj8HZHtTxjovZE=|1603968534\"; _xsrf=3SmcSgji3W2HtHLn3iIcdwBiPvl04WDi; _9755xjdesxxd_=32; YD00517437729195%3AWM_TID=zDc5XyFbzL1FQQBRVVY7nFKogSBzCOiH; YD00517437729195%3AWM_NI=JARFs734My9fIxWZHhfahsRXiDT5ug7fD1ZfZEcet5Hp2GomzEnImq5NxWfk5t8Nudwp%2BmZ8ENkPFqY6Ul5TOdDHqHIpH6%2BCg5sHuK0Vha2vqJ%2F0OfxTuqmrTVeKjJu7em0%3D; YD00517437729195%3AWM_NIKE=9ca17ae2e6ffcda170e2e6eea8f368b1bd9ddab67eb4928bb2c55b839a9f86d15dba90a993d141aca8bd82cd2af0fea7c3b92a8a9f9ea7f23df1ec839abc6e8ba899bbcb64a6bfa894d154ab9c9cd6f86df5ba8cd2d96e9a9bad86f73f8cb0fea8ee67ba9ba2aaf07bf792b8b8e561f7acf8aff870b5b0e58ed9348792a5b1c57b9bea9b8db8608dbc8ca9f4448bb088d7aa49a5ebbda9f841f48aa882b17eb396e1d1ce6ebc8cbb92f07a988aabace942b7e9ab8eb337e2a3; gdxidpyhxdE=UTT43Heojmg%5CjScYgzOfVeEP35mzE8rI%5CM8yOJIw9E%5CM%2BLfGbeHsD3U%2FhVOMSqjfAw9%2FJMCsEsKUvDk0jCQfN5bEGIhzPDATshbpUO%2F0ggqReKjH0rDNYVoaanMyt3cqp6uCJAj8c%2BEsqTMxIDN1K7%2FcSic5SKdZMUSWSEH45GA%5CY%2Br6%3A1663723913499; Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49=1663057627,1663234404,1663637981,1663748189; captcha_session_v2=2|10|101664439040|18captcha_session_v2|88a04rRHdSWVAzZnZHTWw0bmlCWktSOGZjRDY4Q1VYeFJIT3YyVzN5Vk12MjNTQlZCUFF1UkV1ZVloQjNzQ3FJdA==|3479878ea13ce2530d023aa27acc88d0dfdab6023dfb9a52972dfab35853c00a; KLBRSID=57358d62405ef24305120316801fd92a|1664441728|1664439040"
            },
            cookies=getattr(self, 'cookies'),
            method='get'
        )
        cookies = requests.utils.dict_from_cookiejar(self.session.cookies)
        setattr(self, 'cookies', cookies)

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

    def _prep_request(self):
        # 初始化爬虫状态
        self.__init_authentication()
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
            elif isinstance(v, bool):
                self.request[k] = v
            else:
                self.request[k] = v if v else getattr(self, k)
        # 执行之后更新已执行标识
        self._prep_request_flag = True

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        loads = json.loads(resp_str)
        return loads['data']['prodSaleList']

    def _row_processor(self, row: dict) -> dict:
        return row

    def _if_end(self, response: Response) -> typing.Optional[bool]:
        """
        在处理完一次请求之后执行
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
                        elif isinstance(v, bool):
                            self.request[k] = v
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
        self.request['cookies'] = getattr(self, 'cookies')


xyyh_crawl_mobile = XyyhCrawlRequest(
    requests_iter=[
        # 本行理财产品
        {
            'request': {
                'url': 'https://g.cib.com.cn/portal/api/commons/entry/prodsale/list?flag=1 ',
                'headers': '',
                'method': '',
                'verify': False
            },
            'title': '本行理财产品'
        },
    ],
    default_headers={
        "Host": "g.cib.com.cn", "Connection": "keep-alive", "Pragma": "no-cache",
        "Accept": "application/json", "Cache-Control": "no-cache",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x6700143B) WindowsWechat(0x6307062c)",
        # "Expires": "Sat, 01 Jan 2000 000000 GMT",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty",
        "Referer": "https//g.cib.com.cn/mobile/product/index", "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7"
    },
    default_method='get',
    default_identifier='xyyh',
    default_check_props=['logId', 'cpbm']
)

__all__ = ['xyyh_crawl_mobile']
