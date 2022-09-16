import json
import math
import time

import requests
from requests import Session

from payh_config import PAYH_PC_REQUEST, SLEEP_SECOND, MISSING_CODE


def __do_request(session: Session, request: dict, page_num: int):
    response = session.request(
        method=request['method'],
        url=request['url'],
        data=request['data'](page_num),
        headers=request['headers'],
        verify=False
    )
    time.sleep(SLEEP_SECOND)
    resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
    loads = json.loads(resp_str)
    return loads


def process_payh_pc(session: Session):
    rows = []
    for title, request in PAYH_PC_REQUEST.items():
        print(title)
        if title in ['银行理财', '银行公司理财', '对公结构性存款', '个人结构性存款', '公募基金']:
            continue
        int_resp = __do_request(session, request, 1)
        total_page = math.ceil(int(int_resp['data']['totalSize']) / int(request['data'](1)['pageSize']))
        for page in range(1, total_page + 1):
            page_resp = __do_request(session, request, page)
            for prd in page_resp['data']['superviseProductInfoList']:
                print(prd)
                row = request['row_mapping'](prd)
                print(row)
                rows.append(row)
    print('MISSING_CODE:', json.dumps(MISSING_CODE).encode().decode('unicode_escape'))


if __name__ == '__main__':
    session = requests.session()
    process_payh_pc(session)
    session.close()
