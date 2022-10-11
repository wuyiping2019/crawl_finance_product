import json
import math
import re
import time
from typing import List

from bs4 import BeautifulSoup
from requests import Response

from config_parser import crawl_config
from crawl_utils.common_utils import extract_content_between_content
from crawl_utils.crawl_request import ConfigurableCrawlRequest
from crawl_utils.mappings import FIELD_MAPPINGS
from crawl_utils.string_utils import remove_space
from 华夏银行_完成.hxyh_config import PC_REQUEST_INFO, SLEEP_SECOND


class HxyhPCCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super().__init__(name='华夏银行PC端')
        self.page_no = None
        self.total_page = None
        self.request = {}

    def set_request_params(self):
        self.request['params'] = {
            'pageNum': self.page_no,
            'pageSize': 40
        }

    def _pre_crawl(self):
        self.request = PC_REQUEST_INFO['request']
        self.mask = PC_REQUEST_INFO['identifier']
        self.field_value_mapping = PC_REQUEST_INFO['field_value_mapping']
        self.field_name_2_new_field_name = PC_REQUEST_INFO['field_name_2_new_field_name']
        self.check_props = PC_REQUEST_INFO['check_props']
        if crawl_config.state == 'DEV':
            self.total_page = 1

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.set_request_params()

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        loads = json.loads(resp_str)
        rows = loads['body']['list']
        if self.total_page is None:
            self.total_page = math.ceil(int(float(loads['body']['total'])) / loads['body']['pageSize'])
        return rows

    def _config_end_flag(self):
        if self.page_no is not None and self.total_page is not None and self.total_page == self.page_no:
            self.end_flag = True

    def _row_processor(self, row: dict) -> dict:
        self._parse_cpsms(row)
        return row

    def _row_post_processor(self, row: dict):
        return row

    def _parse_cpsms(self, row: dict):
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
            pass
        else:
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


if __name__ == '__main__':
    crawl_pc = HxyhPCCrawlRequest()
    crawl_pc.do_crawl()
