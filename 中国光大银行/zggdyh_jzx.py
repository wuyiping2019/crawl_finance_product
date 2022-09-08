import json
import re
import time

import requests
from bs4 import BeautifulSoup

from utils.html_utils import check_attr
from utils.string_utils import remove_space
from zggdyh_config import SLEEP_SECOND, PATTERN_Z, PATTERN_E


def process_jzx_type(session):
    """
    处理
    网址http://www.cebbank.com/site/gryw/yglc/lccp49/index.html
    中收益类型为《净值型》的数据
    :param session:
    :return:
    """
    domain_url = 'http://www.cebbank.com/'
    url = 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/yglcAction!listProduct.action'
    get_data = lambda \
            page: f'codeOrName=&TZBZMC=&sylxArr%5B%5D=02&SFZS=&qxrUp=Y&qxrDown=&dqrUp=&dqrDown=&qdjeUp=&qdjeDown=&qxUp=&qxDown=&yqnhsylUp=&yqnhsylDown=&page={page}&pageSize=12&channelIds%5B%5D=yxl94&channelIds%5B%5D=ygelc79&channelIds%5B%5D=hqb30&channelIds%5B%5D=dhb2&channelIds%5B%5D=cjh&channelIds%5B%5D=gylc70&channelIds%5B%5D=Ajh67&channelIds%5B%5D=Ajh84&channelIds%5B%5D=901776&channelIds%5B%5D=Bjh91&channelIds%5B%5D=Ejh99&channelIds%5B%5D=Tjh70&channelIds%5B%5D=tcjh96&channelIds%5B%5D=ts43&channelIds%5B%5D=ygjylhzhMOM25&channelIds%5B%5D=yxyg87&channelIds%5B%5D=zcpzjh64&channelIds%5B%5D=wjyh1&channelIds%5B%5D=smjjb9&channelIds%5B%5D=ty90&channelIds%5B%5D=tx16&channelIds%5B%5D=ghjx6&channelIds%5B%5D=ygxgt59&channelIds%5B%5D=wbtcjh3&channelIds%5B%5D=wbBjh77&channelIds%5B%5D=wbTjh28&channelIds%5B%5D=sycfxl&channelIds%5B%5D=cfTjh&channelIds%5B%5D=jgdhb&channelIds%5B%5D=tydhb&channelIds%5B%5D=jgxck&channelIds%5B%5D=jgyxl&channelIds%5B%5D=tyyxl&channelIds%5B%5D=dgBTAcp&channelIds%5B%5D=27637097&channelIds%5B%5D=27637101&channelIds%5B%5D=27637105&channelIds%5B%5D=27637109&channelIds%5B%5D=27637113&channelIds%5B%5D=27637117&channelIds%5B%5D=27637121&channelIds%5B%5D=27637125&channelIds%5B%5D=27637129&channelIds%5B%5D=27637133&channelIds%5B%5D=gyxj32&channelIds%5B%5D=yghxl&channelIds%5B%5D=ygcxl&channelIds%5B%5D=ygjxl&channelIds%5B%5D=ygbxl&channelIds%5B%5D=ygqxl&channelIds%5B%5D=yglxl&channelIds%5B%5D=ygzxl&channelIds%5B%5D=ygttg'
    headers = {"Accept": "text/html, */*; q=0.01", "Accept-Encoding": "gzip, deflate",
               "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", "Cache-Control": "no-cache", "Connection": "keep-alive",
               "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", "Host": "www.cebbank.com",
               "Origin": "http//www.cebbank.com", "Pragma": "no-cache",
               "Referer": "http//www.cebbank.com/site/gryw/yglc/lccp49/index.html",
               "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36",
               "X-Requested-With": "XMLHttpRequest"}
    response = session.post(url=url, data=get_data(1), headers=headers)
    time.sleep(SLEEP_SECOND)
    resp_str = response.text.encode(response.encoding).decode('utf-8')
    soup = BeautifulSoup(resp_str, 'lxml')
    # 获取总页数
    total_page = int(remove_space(soup.select('#totalpage')[0].text))
    rows = []
    for page in range(1, total_page + 1):
        response = session.post(url=url, data=get_data(page), headers=headers)
        time.sleep(SLEEP_SECOND)
        detail_resp_str = response.text.encode(response.encoding).decode('utf-8')
        detail_soup = BeautifulSoup(detail_resp_str, 'html.parser')
        lis = detail_soup.select('.lccp_main_content_tx')[0].select('li')
        for li in lis:
            row = {}
            # 解析 《产品编码、产品名称、产品详情页url》
            a_tag = li.select('a')[0]
            split = remove_space(a_tag['data-analytics-click']).split('-')
            if len(split) == 3:
                # 获取 产品编码
                row['cpbm'] = remove_space(split[2])
                # 获取 产品详情页的url
                try:
                    row['href'] = domain_url + remove_space(a_tag['href'])
                except Exception as e:
                    row['href'] = ''
                # 获取 产品名称
                row['cpmc'] = a_tag['title'].replace(row['cpbm'], '').strip(')').strip('(')

            # 解析 《净值、净值日期》
            select = li.select('.lccp_syl')
            try:
                jz = remove_space(select[0].select('.lccp_ll')[0].text)
                if jz:
                    row['jz'] = jz
            except Exception as e:
                print(e)
            try:
                # 可能会存在净值是--，此时没有净值日期
                extract_list = re.findall(PATTERN_E, remove_space(select[0].text))
                if extract_list:
                    jzrq = extract_list[0]
                    row['jzrq'] = jzrq
            except Exception as e:
                print(f'无法获取{row["cpbm"]}净值日期')

            # 解析 《管理人》
            try:
                # 不一定能找到lccp_cpglr
                lccp_cpglr_tag_list = li.select('.lccp_cpglr')
                glr = ''
                if lccp_cpglr_tag_list:
                    glr = remove_space(lccp_cpglr_tag_list[0].text).strip('产品管理人：')
                else:
                    lccp_syl_tag_list = li.select('.lccp_syl')
                    if lccp_syl_tag_list:
                        for tag in lccp_syl_tag_list:
                            if '产品管理人' in remove_space(tag.text):
                                glr = remove_space(tag.text).strip('产品管理人').strip(':').strip('：')
                                break
                if glr:
                    row['glr'] = glr
            except Exception as e:
                print(f'无法获取{row["cpbm"]}管理人')

            # 解析 《下一个申购日》
            try:
                fls = li.select('.fl')
                for fl in fls:
                    if '申购日' in remove_space(fl.text):
                        xygsgr = remove_space(fl.select('span')[0].text)
                        if xygsgr:
                            row['xygsgr'] = xygsgr
                    if '起点金额' in remove_space(fl.text):
                        qgje = remove_space(fl.select('span')[0].text)
                        if qgje:
                            row['qgje'] = qgje
            except Exception as e:
                print(e)

            # 解析 《风险等级》
            try:
                fxdj = remove_space(li.select('.lc_fxdj')[0].text)
                if fxdj:
                    row['fxdj'] = fxdj
            except Exception as e:
                print(f'无法获取{row["cpbm"]}风险等级')
            rows.append(row)

            # 处理row中href中产品的详情页数据
            if row['href']:
                detail_row = {}
                detail_url = row['href']
                detail_headers = {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                    "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Cache-Control": "no-cache", "Connection": "keep-alive", "Host": "www.cebbank.com",
                    "Pragma": "no-cache", "Referer": "http//www.cebbank.com/site/gryw/yglc/lccp49/index.html",
                    "Upgrade-Insecure-Requests": "1",
                    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36"}
                detail_response = session.get(url=detail_url, headers=detail_headers)
                time.sleep(SLEEP_SECOND)
                detail_resp_str = detail_response.text.encode(detail_response.encoding).decode('utf-8')
                soup = BeautifulSoup(detail_resp_str, 'html.parser')
                lccp_xq_con_1 = soup.select('.lccp_xq_con_1')[0]
                lccp_xq_con_2 = soup.select('.lccp_xq_con_2')[0]
                # 获取 《净值》
                xq_syl_fl = lccp_xq_con_1.select('.xq_syl.fl')
                if xq_syl_fl:
                    syl_sz = xq_syl_fl[0].select('.syl_sz')
                    if syl_sz:
                        jz = remove_space(syl_sz[0].text)
                        if jz:
                            detail_row['jz'] = jz
                    syl_wz = xq_syl_fl[0].select('.syl_wz')
                    if syl_wz:
                        findall = re.findall(PATTERN_Z, remove_space(syl_wz[0].text))
                        findall += re.findall(PATTERN_E, remove_space(syl_wz[0].text))
                        if findall:
                            jzrq = findall[0]
                            if jzrq:
                                detail_row['jzrq'] = jzrq
                # 获取 《起点金额》
                xq_qgje_fl = lccp_xq_con_1.select('.xq_qgje.fl')
                if xq_qgje_fl:
                    qgje_sz_dsje_sz = xq_qgje_fl[0].select('.qgje_sz.dsje_sz')
                    if qgje_sz_dsje_sz:
                        qgje = remove_space(qgje_sz_dsje_sz[0].text)
                        if qgje:
                            detail_row['qgje'] = qgje

                # 获取 《风险等级》
                xq_fxdj_fl = lccp_xq_con_1.select('.xq_fxdj.fl')
                if xq_fxdj_fl:
                    qgje_sz = xq_fxdj_fl[0].select('.qgje_sz')
                    if qgje_sz:
                        fxdj = remove_space(qgje_sz[0].text)
                        if fxdj:
                            detail_row['fxdj'] = fxdj
                # 获取 《理财期限》
                xq_lcqx_fl_tags = lccp_xq_con_1.select('.xq_lcqx.fl')
                for tag in xq_lcqx_fl_tags:
                    tag_text = remove_space(tag.text)
                    if '理财期限' in tag_text:
                        inner_tags = tag.select('.lcqx_sz')
                        if inner_tags:
                            tzqx = remove_space(inner_tags[0].text)
                            if tzqx:
                                detail_row['tzqx'] = tzqx
                #  ============================================================
                # 获取 《管理人》
                glr_tag = lccp_xq_con_2.select('.fdsy_con_name.fdsy_cpglr.fl')
                if glr_tag:
                    splits = remove_space(glr_tag[0].text).split("：")
                    if len(splits) == 2 and splits[0] == '产品管理人':
                        glr = splits[1]
                        if glr:
                            detail_row['glr'] = glr
                names = []
                values = []
                name_tags = lccp_xq_con_2.select('.fdsy_con_name.fl') if len(
                    lccp_xq_con_2.select('.fdsy_con_name.fdsy_cpglr.fl')) == 0 else [] \
                                                                                    + lccp_xq_con_2.select(
                    '.fdsy_con_name1.fl')
                value_tags = lccp_xq_con_2.select('.fdsy_con_nr.fl') + lccp_xq_con_2.select('.fdsy_con_nr1.fl')
                for name_tag in name_tags:
                    lis = name_tag.select('li')
                    for li in lis:
                        name_text = remove_space(li.text)
                        names.append(name_text)
                for value_tag in value_tags:
                    lis = value_tag.select('li')
                    # 去掉lis中存在同名class的value 保留style='display: block;'
                    for li in lis:
                        if check_attr(li, 'class') and li['class'][0] == 'dgdz':
                            continue
                        else:
                            values.append(remove_space(li.text))
                for name, value in zip(names, values):
                    if name == '递增金额':
                        if value:
                            detail_row['dzje'] = value
                    elif name == '产品种类':
                        if value:
                            detail_row['zyms'] = value  # 运作模式
                    elif name == '币种':
                        if value:
                            detail_row['mjbz'] = value
                    elif name == '下一个申购日':
                        if value:
                            detail_row['xygsgr'] = value
                # 获取 《产品说明书》
                try:
                    href_ = soup.select('.cpsms_nr')[0].select('a')[0]['href']
                    if href_:
                        detail_row['cpsms'] = json.dumps({
                            'url': 'http://static2.cebbank.com' + href_,
                            'title': remove_space(soup.select('.cpsms_nr')[0].select('a')[0].text)
                        }).encode().decode('unicode_escape')
                except Exception as e:
                    print(f'无法获取{row["cpbm"]}产品说明书')
                # 获取 《剩余额度》
                try:
                    remain_account = remove_space(soup.select('#remainAccount')[0].text)
                    if remain_account:
                        detail_row['syed'] = remain_account
                except Exception as e:
                    print(f'无法获取{row["cpbm"]}剩余额度')
                row.update(detail_row)
                del row['href']
            # 获取 净值型产品的历史净值轨迹数据
            lsjz_url = 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/yglcAction!getLsjz.action'
            get_lsjz_data = lambda cpbm: {'cpcode': cpbm}
            lsjz_headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache", "Connection": "keep-alive",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Host": "www.cebbank.com", "Origin": "http//www.cebbank.com", "Pragma": "no-cache",
                "Referer": "http//www.cebbank.com/site/gryw/yglc/lsjz/index.html?cpcode=EB1066",
                "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36",
                "X-Requested-With": "XMLHttpRequest"
            }
            lsjz_response = session.post(url=lsjz_url, data=get_lsjz_data(row['cpbm']), headers=headers)
            time.sleep(SLEEP_SECOND)
            try:
                lsjz = []
                loads = ''
                loads = json.loads(lsjz_response.text.encode(lsjz_response.encoding).decode(
                    'utf-8')) if lsjz_response.encoding is not None else json.loads(lsjz_response.text)
                for item in loads['lsjzMap']:
                    lsjz.append({
                        'jzrq': item['JZRQ'],
                        'jz': item['JZ']
                    })
                try:
                    if lsjz:
                        lsjz.sort(key=lambda x: x['jzrq'], reverse=True)
                except Exception as e:
                    pass
                if lsjz:
                    row['lsjz'] = json.dumps(lsjz).encode().decode('unicode_escape')
            except Exception as e:
                print(f'无法获取{row["cpbm"]}的历史净值轨迹')

            print(row)
    return rows


if __name__ == '__main__':
    # 测试 获取净值型的产品
    # 1.获取净值型产品的历史净值
    pass
