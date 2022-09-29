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
        rows = loads['body']['list']
        if not getattr(self, 'total_page', None):
            setattr(self, 'total_page', math.ceil(int(float(loads['body']['total'])) / loads['body']['pageSize']))
        return rows

    def _row_processor(self, row: dict) -> dict:
        # 业绩比较基准
        yjbjjz = ''
        if row['licaiVieldLow'] not in [0, '0'] or row['licaiVieldMax'] not in [0, '0']:
            if row['licaiVieldMiddle'] not in [0, '0']:
                yjbjjz = {
                    'title': '预期年化收益率',
                    'value': f"{format(float(row['licaiVieldLow']) * 100, '.2f')}%/{format(float(row['licaiVieldMiddle']) * 100, '.2f')}%/{format(float(row['licaiVieldMax']) * 100, '.2f')}%"
                }
            else:
                yjbjjz = {
                    'title': '预期年化收益率',
                    'value': f"{format(float(row['licaiVieldLow']) * 100, '.2f')}%~{format(float(row['licaiVieldMax']) * 100, '.2f')}%"
                }
        if row['currencyVield'] not in [0, '0']:
            yjbjjz = {
                'title': '7日年化收益率',
                'value': f"{format(float(row['currencyVield']) * 100, '.4f')}%"
            }
        if yjbjjz:
            row['yjbjjz'] = json.dumps(yjbjjz).encode().decode('unicode_escape')
        self._parse_cpsms(row)
        return row

    def _parse_cpsms(self, row: dict):
        where = 'hxyh_pc._parse_pc'
        # 产品说明书
        cpsms = {
            'title': '',
            'url': f'https://www.hxb.com.cn/lcpdf/{row["href"]}.html'
        }
        response_sms = self.session.get(url=cpsms['url'], headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate, br", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache", "Connection": "keep-alive", "Host": "www.hxb.com.cn", "Pragma": "no-cache",
            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"", "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Site": "none", "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"})
        time.sleep(SLEEP_SECOND)
        if response_sms.status_code == 200:
            log(self.logger, 'debug', where, f"成功获取到{row['licaiName']}的产品说明书,响应码:{response_sms.status_code}")
        else:
            log(self.logger, 'warn', where,
                f"无法获取{row['licaiName']}的产品说明书,响应码:{response_sms.status_code},url:{cpsms['url']}")
            return
        time.sleep(SLEEP_SECOND)
        resp_sms_str = response_sms.text.encode(
            response_sms.encoding).decode('utf-8') if response_sms.encoding else response_sms.text
        soup = BeautifulSoup(resp_sms_str, 'lxml')
        title = remove_space(soup.select('h2')[0].text)
        cpsms['title'] = title
        row['cpsms'] = json.dumps(cpsms).encode().decode('unicode_escape')
        # 产品简称
        try:
            tr_tags = soup.select('.promise-table')[0].select('tr')
            for tr_tag in tr_tags:
                tds = tr_tag.select('td')
                if len(tds) == 2:
                    td_left = remove_space(tds[0].text)
                    if td_left == '产品代码':
                        cpbm = remove_space(tds[1].text)
                        row[FIELD_MAPPINGS['产品编码']] = cpbm
                    if td_left == '全国银行业理财登记系统登记编码':
                        djbm = tds[1].text. \
                            replace('投资者可以依据该登记编码在中国理财网', ''). \
                            replace('www.chinawealth.com.cn', ''). \
                            replace('查询产品信息', ''). \
                            replace('(', '').replace(')', '').replace('（', '').replace('）', ''). \
                            strip()
                        row[FIELD_MAPPINGS['登记编码']] = djbm
                    if td_left == '产品管理人':
                        glr = remove_space(tds[1].text)
                        row[FIELD_MAPPINGS['管理人']] = glr
                    if td_left == '产品募集方式':
                        mjfs = remove_space(tds[1].text)
                        row[FIELD_MAPPINGS['募集方式']] = mjfs
                    if td_left == '产品运作模式':
                        yzms = remove_space(tds[1].text)
                        row[FIELD_MAPPINGS['运作模式']] = yzms
                    if td_left == '产品投资性质':
                        tzxz = remove_space(tds[1].text)
                        row[FIELD_MAPPINGS['投资性质']] = tzxz
                    if td_left == '产品收益类型':
                        sylx = remove_space(tds[1].text)
                        row[FIELD_MAPPINGS['收益类型']] = sylx
                    if td_left == '投资及收益币种':
                        bz = remove_space(tds[1].text)
                        row[FIELD_MAPPINGS['币种']] = bz
                    if td_left == '产品风险评级':
                        fxdj = extract_content_between_content(remove_space(tds[1].text), '本产品为', '理财产品')
                        row[FIELD_MAPPINGS['风险等级']] = fxdj
                    if td_left == '募集期':
                        pattern = re.compile(r'\d{4}年\d{1,2}月\d{1,2}日－\d{4}年\d{1,2}月\d{1,2}日')
                        findall = re.findall(pattern, remove_space(tds[1].text))
                        if findall:
                            split = findall[0].split('－')
                            if len(split) == 2:
                                mjqsrq = split[0]
                                mjjsrq = split[1]
                                row[FIELD_MAPPINGS['募集起始日期']] = mjqsrq
                                row[FIELD_MAPPINGS['募集结束日期']] = mjjsrq
                    if td_left == '成立日':
                        clr = remove_space(tds[1].text)
                        row[FIELD_MAPPINGS['成立日']] = clr
                    if td_left == '发行范围':
                        xsqy = remove_space(tds[1].text)
                        row[FIELD_MAPPINGS['销售区域']] = xsqy
        except Exception as e:
            pass

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
        self.request['params'] = getattr(self, 'requests_iter')[getattr(self, 'current_request_index')]['request'][
            'params'](
            getattr(self, 'page_no'))


def process_source_name(source_code):
    source_name = ""
    if not source_code and source_code != 0:
        source_name = ""
    else:
        if '-1' in source_code:
            source_name = source_name + "全部、"
        if '0' in source_code:
            source_name = source_name + "柜面、"
        if '1' in source_code and source_code != '-1':
            source_name = source_name + "电话、"
        if '2' in source_code:
            source_name = source_name + "网银、"
        if '3' in source_code:
            source_name = source_name + "手机、"
        if '4' in source_code:
            source_name = source_name + "支付融资、"
        if 'B' in source_code:
            source_name = source_name + "智能柜台、"
        if 'C' in source_code:
            source_name = source_name + "微信银行、"
        if '8' in source_code:
            source_name = source_name + "e社区、"
        return source_name.strip('、')


hxyh_crawl_pc = HxyhCrawlRequest(
    requests_iter=[
        # 华夏银行-爬取产品列表
        {
            'request': {
                'url': 'https://www.hxb.com.cn/precious-metal-mall/LchqController/findPage',
                'method': 'post',
                'params': lambda page_num: {
                    'pageNum': page_num,
                    'pageSize': 40

                },
                'json': {
                    "licaiName": "",
                    "licaiBuyOriginOrderFlag": "",
                    "licaiVieldMaxOrderFlag": "",
                    "licaiTimeLimitMaxOrderFlag": ""
                },
                'headers': {"Accept": "application/json, text/javascript, */*; q=0.01",
                            "Accept-Encoding": "gzip, deflate, br",
                            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", "Cache-Control": "no-cache",
                            "Connection": "keep-alive",
                            "Content-Length": "105", "Content-Type": "application/json;charset=UTF-8",
                            "Host": "www.hxb.com.cn",
                            "Origin": "https//www.hxb.com.cn", "Pragma": "no-cache",
                            "Referer": "https//www.hxb.com.cn/grjr/lylc/zzfsdlccpxx/index.shtml",
                            "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
                            "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"", "Sec-Fetch-Dest": "empty",
                            "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
                            "X-Requested-With": "XMLHttpRequest"
                            },

            },
            'identifier': 'hxyh',
            'field_value_mapping': {
                'licaiChannelSource': process_source_name,
                'personalFirstBuyLimit': lambda x: str(int(float(x) * 10000)) + '元'
            },

            'field_name_2_new_field_name': {
                'licaiCode': FIELD_MAPPINGS['产品编码'],
                'licaiName': FIELD_MAPPINGS['产品名称'],
                'licaiState': FIELD_MAPPINGS['产品状态'],
                'licaiNewNet': FIELD_MAPPINGS['净值'],
                'licaiRiskLevel': FIELD_MAPPINGS['风险等级'],
                'licaiTimeLimit': FIELD_MAPPINGS['投资期限'],
                'buyBeginDate': FIELD_MAPPINGS['产品起始日期'],
                'licaiExpireDay1': FIELD_MAPPINGS['产品结束日期'],
                'licaiChannelSource': FIELD_MAPPINGS['销售渠道'],
                'personalFirstBuyLimit': FIELD_MAPPINGS['起购金额'],
                '业绩比较基准': FIELD_MAPPINGS['业绩比较基准'],
                '产品说明书': FIELD_MAPPINGS['产品说明书']
            },
            'check_props': ['logId', 'cpbm'],
            'title': '华夏银行-PC'
        },
    ]
)

__all__ = ['hxyh_crawl_pc']
