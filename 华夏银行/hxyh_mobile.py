import json
import time

import requests
from requests import Session

from hxyh_config import HXYH_MOBILE_REQUEST, SLEEP_SECOND
from utils.custom_exception import CustomException
from utils.mappings import row_mapping, FIELD_MAPPINGS


def do_request(session: Session, page_num: int, type: str) -> tuple:
    response = session.request(method=HXYH_MOBILE_REQUEST[type]['method'],
                               url=HXYH_MOBILE_REQUEST[type]['url'],
                               headers=HXYH_MOBILE_REQUEST[type]['headers'],
                               json=HXYH_MOBILE_REQUEST[type]['data'](page_num),
                               verify=False)
    time.sleep(SLEEP_SECOND)
    cookies = requests.utils.dict_from_cookiejar(response.cookies)
    resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
    loads = json.loads(resp_str)
    loads_response = loads['response']
    if isinstance(loads_response, str):
        return cookies, json.loads(loads_response)
    elif isinstance(loads_response, dict):
        return cookies, response.cookies['LESSION'], loads_response


def process_hxyh_mobile(session: Session):
    cookies, ini_resp = do_request(session, 1, 'prd_list')
    total_page = int(float(ini_resp['data']['totalPage']))
    rows = []
    for page_num in range(1, total_page + 1):
        # 获取指定页面的数据
        cookies, page_loads = do_request(session, page_num, 'prd_list')
        prd_list = page_loads['data']['list']
        for prd in prd_list:
            print(prd)
            row = row_mapping(['投资期限', '产品编码', '产品名称'], ['productLimit', 'productCode', 'productName'], prd)
            # 处理业绩比较基准
            income_type = prd.get('incomeType', '')
            if income_type == '1':
                row[FIELD_MAPPINGS['业绩比较基准']] = json.dumps({
                    'title': '预期年化收益率',
                    'value': prd['productExpectIncomeRate']
                })
            else:
                raise CustomException(None, f'出现没有考虑的{income_type}')
            # response_detail = session.request(method=HXYH_MOBILE_REQUEST['prd_detail']['method'],
            #                                   url=HXYH_MOBILE_REQUEST['prd_detail']['url'],
            #                                   json=HXYH_MOBILE_REQUEST['prd_detail']['data'](row['cpbm']),
            #                                   cookies=cookies)
            # print(response_detail)
            rows.append(row)


if __name__ == '__main__':
    session = requests.session()
    try:
        process_hxyh_mobile(session)
    except Exception as e:
        raise e
    finally:
        session.close()
