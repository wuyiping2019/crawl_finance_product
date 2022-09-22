# 公募基金
import re

import requests
from bs4 import BeautifulSoup

from utils.crawl_request_utils import CrawlRequest
from utils.global_config import get_table_name, get_sequence_name, get_trigger_name
from utils.html_utils import parse_table

__url = 'http://www.cgbchina.com.cn/Channel/22598502'
__method = 'POST'
__data = lambda page_no: {
    'proName': '',
    'proCode': '',
    'curPage': f'{page_no}'
}
__headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", "Cache-Control": "no-cache",
    "Host": "www.cgbchina.com.cn", "Pragma": "no-cache", "Proxy-Connection": "keep-alive",
    "Referer": "http//www.cgbchina.com.cn/Channel/25518271", "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
}

__field_mapping_funcs = {
}
__mapping_dicts = {

}
__transformed_keys = {
    '产品代码': '产品编码',
    '产品名称': '产品名称',
    '产品类型': '募集方式',
    '产品管理人': '管理人',
    '销售区域': '销售区域',
    '风险等级': '风险等级',
    '产品状态': '产品状态',
    '产品净值': '净值'
}


def __switch_page_func(self):
    self.request['data'] = __data(self.page_no)


def __to_rows(response):
    resp_str = response.content.decode(response.encoding) \
        if response.encoding \
        else response.text
    soup = BeautifulSoup(resp_str, 'lxml')
    tables = soup.select('table')
    if tables:
        table = tables[0]
        rows = parse_table(table,
                           ['序号',
                            '产品代码',
                            '产品名称',
                            '产品类型',
                            '产品管理人',
                            '募集方式',
                            '销售区域',
                            '风险等级',
                            '产品状态',
                            '产品净值',
                            '产品详情'],
                           callbacks={},
                           extra_attrs={})
        return rows


def do_crawl(session, conn, log_id):
    response = session.request(method=__method, url=__url, headers=__headers, data=__data(1))
    resp_str = response.content.decode(response.encoding) \
        if response.encoding \
        else response.text
    findall = re.findall(r'第\d/(\d+)页', resp_str)
    print(findall)
    if findall:
        total_page = int(findall[0])
        crawl = CrawlRequest(method=__method, url=__url, switch_page_func=__switch_page_func, data=__data(1),
                             headers=__headers,
                             field_mapping_funcs=__field_mapping_funcs,
                             mapping_dicts=__mapping_dicts,
                             transformed_keys=__transformed_keys,
                             missing_code_sample=1,
                             target_table=get_table_name('gfyh'),
                             sequence_name=get_sequence_name('gfyh'),
                             trigger_name=get_trigger_name('gfyh'),
                             log_id=log_id,
                             session=session,
                             to_rows=__to_rows,
                             total_page=total_page,
                             sleep_second=1,
                             connection=conn)
        while not crawl.end_flag:
            crawl.do_crawl()
        return crawl.processed_rows


if __name__ == '__main__':
    session = requests.session()
    do_crawl(session, None, 1)
    session.close()
