import json
import math

import requests
from requests import Session

from utils.crawl_request_utils import CrawlRequest
from utils.global_config import get_table_name, get_sequence_name, get_trigger_name

__method = 'post'
__url = 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do'
__page_size = 20
__data = lambda page_no: {
    'tableIndex': 'table08',
    'dataType': '08',
    'tplId': 'tpl08',
    'pageNum': f'{page_no}',
    'pageSize': f'{__page_size}',
    'channelCode': 'C0002',
    'access_source': 'PC',
}
__headers = {"accept": "*/*", "accept-encoding": "gzip, deflate, br", "accept-language": "zh-CN,zh;q=0.9",
             "content-length": "99", "content-type": "application/x-www-form-urlencoded;charset=utf-8",
             "cookie": "WEBTRENDS_ID=298ff81b8809ac6d2841663310821456; NGWhitelist=100601,100602; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHk977HoFNHTJYk8DeBGXdlDCJWt5Pcn; WT-FPC=id=298ff81b8809ac6d2841663310821456lv=1663310826636ss=1663310821456fs=1663310821456pn=4vn=1; BSFIT4_EXPIRATION=1663678913274; BSFIT4_DEVICEID=GfP-kTREi3De0HCgo2ur2MZ13Bex9EvaFeVCW8v7RDXIY8ZxI-THQ2ZI11BDzmkRU0HU4cPFXpK534XfD1802lNGifw2pDwx8FOFidsyWolgILVtvuKlRg59V7XPTcS_JSxn2wOiMc8SVjo56uBNdCP9rKthAYhX",
             "origin": "https//ebank.pingan.com.cn",
             "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
             "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
             "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"", "sec-fetch-dest": "empty",
             "sec-fetch-mode": "cors", "sec-fetch-site": "same-origin",
             "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"}
__field_mapping_funcs = {
    'minAmount': lambda x: str(x) + '元',
}
__mapping_dicts = {
    'riskLevel': {
        '': '',
    },

}
__transformed_keys = {
    'investmentScope': '投资性质',
    'riskLevel': '风险等级',
    'prdName': '产品名称',
    'prdManager': '管理人',
    'prdCode': '产品编码',
    'status': '产品状态'

}


def __switch_page_func(self):
    self.request['data'] = __data(self.page_no)


def __to_rows(response):
    resp_str = response.text.encode(response.encoding).decode('utf-8') \
        if response.encoding else response.text
    loads = json.loads(resp_str)
    return loads['data']['superviseProductInfoList']


# 对公结构性存款
def do_crawl(session: Session, connection=None) -> CrawlRequest:
    # 获取总页数
    response = session.request(method=__method, url=__url, data=__data(1), headers=__headers)
    resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
    loads = json.loads(resp_str)
    total_page = math.ceil(int(float(loads['data']['totalSize'])) / __page_size)
    print(f'total_page:{total_page}')
    crawl = CrawlRequest(
        method=__method,
        url=__url,
        data=__data(1),
        headers=__headers,
        field_mapping_funcs=__field_mapping_funcs,
        mapping_dicts=__mapping_dicts,
        transformed_keys=__transformed_keys,
        missing_code_sample=1,
        target_table=get_table_name('payh'),
        sequence_name=get_sequence_name('payh'),
        trigger_name=get_trigger_name('payh'),
        log_id=1,
        to_rows=__to_rows,
        total_page=total_page,
        switch_page_func=__switch_page_func,
        connection=connection
    )
    while not crawl.end_flag:
        crawl.do_crawl()
        #########
        print(len(crawl.processed_rows))
        #########
    ######################
    for row in crawl.processed_rows:
        print(row)
    ######################
    crawl.do_save()
    # 表示调用该方法时 没有传入connection对象 则CrawlRequest对象内部自己创建了Connection对象
    # 返回之前提交事务
    if connection is None:
        crawl.connection.commit()
    # 表示调用该方法时 手动传入了connection对象 则CrawlRequest对象内部没有自己创建Connection对象
    # 此时事务由传入者控制是否提交以及什么时候提交
    # 返回CrawlRequest对象
    return crawl


if __name__ == '__main__':
    session = requests.session()
    crawl = do_crawl(session)
    crawl.connection.commit()
    print(json.dumps(crawl.mark_code_mapping).encode().decode('unicode_escape'))
    crawl.close()
