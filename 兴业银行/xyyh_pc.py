import json
import time

import requests
from bs4 import BeautifulSoup
from requests import Session

from utils.custom_exception import cast_exception
from utils.html_utils import parse_table
from xyyh_config import PC_BHLCCP_REQUEST, SLEEP_SECOND


# https://www.cib.com.cn/cn/personal/wealth-management/xxcx/table/
def process_xyyh_pc(session: Session):
    # 本行理财产品
    bhlccp_response = session.request(method=PC_BHLCCP_REQUEST['method'], headers=PC_BHLCCP_REQUEST['headers'],
                                      json=PC_BHLCCP_REQUEST['data'], url=PC_BHLCCP_REQUEST['url'])
    time.sleep(SLEEP_SECOND)
    bhlccp_resp_str = bhlccp_response.text.encode(bhlccp_response.encoding).decode(
        'utf-8') if bhlccp_response.encoding else bhlccp_response.text
    try:
        soup = BeautifulSoup(bhlccp_resp_str, 'lxml')
        table_tags = soup.select('table')
        if table_tags:
            table_tag = table_tags[0]
            rows = parse_table(
                table=table_tag,
                col_names=['pass', '募集方式', '产品名称', '银行业理财信息登记系统登记编码', '产品代码', '产品简称', '代销风险等级', '发行机构', 'pass', '产品类型'],
                callbacks={},
                extra_attrs={},
                head_tag='td'
            )
            for row in rows:
                print(row)
    except Exception as e:
        exception = cast_exception(e)
        raise exception


if __name__ == '__main__':
    session = requests.session()
    process_xyyh_pc(session)
    session.close()
