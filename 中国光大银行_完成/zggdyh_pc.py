import json
import random
import re
import time
from typing import List
from bs4 import BeautifulSoup
from requests import Response
from crawl_utils.crawl_request import ConfigurableCrawlRequest, RowFilter
from crawl_utils.db_utils import getLocalDate
from crawl_utils.html_utils import parse_table
from crawl_utils.logging_utils import get_logger
from crawl_utils.string_utils import remove_space
from 中国光大银行_完成.zggdyh_config import MASK, PC_REQUEST_URL, PC_REQUEST_HEADERS, PC_REQUEST_METHOD, PC_REQUEST_DATA, \
    PC_REQUEST_PARAMS, PC_FIELD_NAME_2_NEW_FIELD_NAME

logger = get_logger(__name__)


class ZggdyhPCCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super(ZggdyhPCCrawlRequest, self).__init__(name='中国光大银行PC')
        self.page_no = None
        self.total_page = None

    def _pre_crawl(self):
        self.mask = MASK
        self.check_props = ['logId', 'cpbm', 'bank']
        self.request['url'] = PC_REQUEST_URL
        self.request['headers'] = PC_REQUEST_HEADERS
        self.request['method'] = PC_REQUEST_METHOD
        self.request['params'] = PC_REQUEST_PARAMS

        self.field_name_2_new_field_name = PC_FIELD_NAME_2_NEW_FIELD_NAME

        if self.config.state == 'DEV':
            self.total_page = 1

        self.candidate_check_props = {
            'cpbm': 'cpmc'
        }

        row_filter = RowFilter().set_name('get_detail_info')
        row_filter.filter = self.get_detail_info
        self.add_filter_after_row_processor(row_filter)

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.request['data'] = PC_REQUEST_DATA(self.page_no)

    def _parse_response(self, response: Response) -> List[dict]:
        resp_str = response.text
        soup = BeautifulSoup(resp_str, 'lxml')
        if self.total_page is None:
            self.total_page = int(remove_space(soup.select('#totalpage')[0].text))
        table = soup.select('table')[0]
        rows = parse_table(table, None, None, None, 'td')
        link_tags = soup.select('a')
        href_dict = {}
        for link in link_tags:
            if 'class' in link.attrs.keys() and 'lb_title' in link.attrs['class']:
                href = 'http://www.cebbank.com' + link.attrs['href']
                link_text = remove_space(link.text)
                href_dict[link_text] = href
        for row in rows:
            if row['产品名称'] in href_dict.keys():
                row['href'] = href_dict[row['产品名称']]
        return rows

    def _config_end_flag(self):
        if self.page_no is not None and self.total_page is not None and self.page_no == self.total_page:
            self.end_flag = True

    def _row_processor(self, row: dict) -> dict:
        row['发售起始日期'] = row['销售日期'].split('止')[0].strip('起:').strip()
        row['发售截止日期'] = row['销售日期'].split('止')[1].strip(':').strip()
        row['业绩比较基准'] = json.dumps({
            'title': '预期年化收益率',
            'value': row['参考收益'].replace('预期年化收益率', '')
        }).encode().decode('unicode_escape')
        search = re.search(r'\(.+\)', row['产品名称'])
        if search:
            cpbm = row['产品名称'][search.start() + 1: search.end() - 1]
            row['产品编码'] = cpbm
            row['产品名称'] = row['产品名称'].replace(row['产品名称'][search.start():search.end()], '')
        return row

    def _row_post_processor(self, row: dict):
        row['logId'] = self.log_id
        row['createTime'] = getLocalDate()
        row['bank'] = '中国光大银行'
        return row

    def get_detail_info(self, row: dict):
        # 处理href
        href = row['href']
        response = self.session.request(method='get', url=href, headers=PC_REQUEST_HEADERS)
        time.sleep(1)
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        soup = BeautifulSoup(resp_str, 'lxml')
        # 解析两个并排的div 一个是key 一个是value
        div_tags = soup.select('div')
        for div in div_tags:
            select = div.select('div')
            if not select:
                # 表明到达根div
                div_text = remove_space(div.text)
                parent_tag = div.parent
                sub_tags = parent_tag.select('div')
                if div_text == '起点金额':
                    row['起点金额'] = remove_space(sub_tags[1].text) if len(sub_tags) == 2 else ''
                elif div_text == '预期年化收益率':
                    row['业绩比较基准'] = remove_space(sub_tags[1].text) if len(sub_tags) == 2 else ''
                elif div_text == '理财期限':
                    row['理财期限'] = remove_space(sub_tags[1].text) if len(sub_tags) == 2 else ''
                elif div_text == '起点金额':
                    row['起点金额'] = remove_space(sub_tags[1].text) if len(sub_tags) == 2 else ''
                elif div_text == '风险等级':
                    row['风险等级'] = remove_space(sub_tags[1].text) if len(sub_tags) == 2 else ''
        # 解析两个并排的ul 一个是key列表 一个是value列表
        div_tags = soup.select('.lccp_xq')[0].select('div')
        for div in div_tags:
            ul_tags = div.select('ul')
            if len(ul_tags) == 2:
                li_name = [remove_space(li.text) for li in ul_tags[0].select('li')]
                li_value = [remove_space(li.text) for li in ul_tags[1].select('li')]
                for name, value in zip(li_name, li_value):
                    row[name] = value
        # 获取产品剩余额度
        cpInfo = row['产品编码'] + ",2," if "EB" in row['产品编码'] else row['产品编码'] + ",1,1001"
        fzcp1Info, fzcp2Info = '', ''
        iRandom = str(int(random.random() * 100000000))
        response = self.session.request(
            method='get',
            url='http://www.cebbank.com' + '/eportalapply/jsp/lcys/lcys.jsp?cpInfo=' + cpInfo + "&fzcp1Info=" + fzcp1Info + "&fzcp2Info=" + fzcp2Info + "&random=" + iRandom,
            headers=PC_REQUEST_HEADERS
        )
        time.sleep(1)
        resp_str = remove_space(response.text.encode().decode('utf-8') if response.encoding else response.text)
        resp_str = resp_str.replace('+', '%2B')
        response = self.session.request(
            method='get',
            url=resp_str,
            headers=PC_REQUEST_HEADERS
        )
        resp_str = response.text.encode().decode('utf-8') if response.encoding else response.text
        search = re.search(r'<PrdLimit>(.+)</PrdLimit>', resp_str)
        syed = str(search.groups()[0]) + '元'
        row['剩余额度'] = syed
        del row['href']
        # 解析产品状态
        # todo
        return row


if __name__ == '__main__':
    ZggdyhPCCrawlRequest().init_props(log_id=1).do_crawl()
