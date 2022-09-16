import json
import math
import re
import time

import requests
from bs4 import BeautifulSoup
from requests import Session

from hxyh_config import HXYH_PC_REQUEST, SLEEP_SECOND
from utils.common_utils import extract_content_between_content
from utils.mappings import FIELD_MAPPINGS, row_mapping
from utils.string_utils import remove_space


def do_request(session: Session, page_num: int):
    response = session.request(method=HXYH_PC_REQUEST['method'], url=HXYH_PC_REQUEST['url'](page_num),
                               headers=HXYH_PC_REQUEST['headers'], json=HXYH_PC_REQUEST['data'],verify=False)
    time.sleep(SLEEP_SECOND)
    resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
    return json.loads(resp_str)


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


def process_hxyh_pc(session: Session):
    rows = []
    loads = do_request(session, 1)
    total_count = loads['body']['total']
    page_size = loads['body']['pageSize']
    total_page = math.ceil(total_count / page_size)
    for page in range(1, total_page + 1):
        loads = do_request(session, page)
        prd_list = loads['body']['list']
        for prd in prd_list:
            row = row_mapping(['产品编码', '产品名称', '产品状态', '净值', '风险等级', '投资期限', '产品起始日期', '产品结束日期', '销售渠道', '起购金额'],
                              ['licaiCode', 'licaiName', 'licaiState', 'licaiNewNet', 'licaiRiskLevel',
                               'licaiTimeLimit', 'buyBeginDate', 'licaiExpireDay1', 'licaiChannelSource',
                               'personalFirstBuyLimit'], prd)
            # 销售渠道转码
            row[FIELD_MAPPINGS['销售渠道']] = process_source_name(row['xsqd'])
            # 起购金额
            row[FIELD_MAPPINGS['起购金额']] = str(int(float(row[FIELD_MAPPINGS['起购金额']]) * 10000)) + '元'
            # 业绩比较基准
            yjbjjz = ''
            if prd['licaiVieldLow'] not in [0, '0'] or prd['licaiVieldMax'] not in [0, '0']:
                if prd['licaiVieldMiddle'] not in [0, '0']:
                    yjbjjz = {
                        'title': '预期年化收益率',
                        'value': f"{format(float(prd['licaiVieldLow']) * 100, '.2f')}%/{format(float(prd['licaiVieldMiddle']) * 100, '.2f')}%/{format(float(prd['licaiVieldMax']) * 100, '.2f')}%"
                    }
                else:
                    yjbjjz = {
                        'title': '预期年化收益率',
                        'value': f"{format(float(prd['licaiVieldLow']) * 100, '.2f')}%~{format(float(prd['licaiVieldMax']) * 100, '.2f')}%"
                    }
            if prd['currencyVield'] not in [0, '0']:
                yjbjjz = {
                    'title': '7日年化收益率',
                    'value': f"{format(float(prd['currencyVield']) * 100, '.4f')}%"
                }
            if yjbjjz:
                row[FIELD_MAPPINGS['业绩比较基准']] = json.dumps(yjbjjz).encode().decode('unicode_escape')
            cpsms_url_html = prd['href']
            if cpsms_url_html:
                cpsms = {
                    'title': '',
                    'url': f'https://www.hxb.com.cn/lcpdf/{cpsms_url_html}.html'
                }
                print(cpsms['url'])
                response_sms = session.get(url=cpsms['url'], headers=HXYH_PC_REQUEST['cpsms']['headers'])
                time.sleep(SLEEP_SECOND)
                resp_sms_str = response_sms.text.encode(
                    response_sms.encoding).decode('utf-8') if response_sms.encoding else response_sms.text
                soup = BeautifulSoup(resp_sms_str, 'lxml')
                title = remove_space(soup.select('h2')[0].text)
                cpsms['title'] = title
                row[FIELD_MAPPINGS['产品说明书']] = json.dumps(cpsms).encode().decode('unicode_escape')
                # 从产品说明书中解析出更多的内容 全部使用try except包裹 获取不到也没关系
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
                print(row)
                rows.append(row)
        return rows


if __name__ == '__main__':
    session = requests.session()
    process_hxyh_pc(session)
    session.close()
