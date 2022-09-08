import json
import math
import random
import time

import requests
from bs4 import BeautifulSoup
from requests import Response

from utils.html_utils import check_attr
from zggdyh_config import SLEEP_SECOND
from utils.string_utils import remove_space


def process_yjjz_type(session):
    """
    处理
    网址http://www.cebbank.com/site/gryw/yglc/lccp49/index.html
    中收益类型为《业绩基准》的数据
    :param session:
    :param log_id:
    :return:
    """
    domain_url = 'http://www.cebbank.com/'
    url = 'http://www.cebbank.com/eportal/ui?moduleId=12073&struts.portlet.action=/app/yglcAction!listProduct.action'
    get_data = lambda \
            page: f'codeOrName=&TZBZMC=&sylxArr%5B%5D=01&SFZS=&qxrUp=Y&qxrDown=&dqrUp=&dqrDown=&qdjeUp=&qdjeDown=&qxUp=&qxDown=&yqnhsylUp=&yqnhsylDown=&page={page}&pageSize=12&channelIds%5B%5D=yxl94&channelIds%5B%5D=ygelc79&channelIds%5B%5D=hqb30&channelIds%5B%5D=dhb2&channelIds%5B%5D=cjh&channelIds%5B%5D=gylc70&channelIds%5B%5D=Ajh67&channelIds%5B%5D=Ajh84&channelIds%5B%5D=901776&channelIds%5B%5D=Bjh91&channelIds%5B%5D=Ejh99&channelIds%5B%5D=Tjh70&channelIds%5B%5D=tcjh96&channelIds%5B%5D=ts43&channelIds%5B%5D=ygjylhzhMOM25&channelIds%5B%5D=yxyg87&channelIds%5B%5D=zcpzjh64&channelIds%5B%5D=wjyh1&channelIds%5B%5D=smjjb9&channelIds%5B%5D=ty90&channelIds%5B%5D=tx16&channelIds%5B%5D=ghjx6&channelIds%5B%5D=ygxgt59&channelIds%5B%5D=wbtcjh3&channelIds%5B%5D=wbBjh77&channelIds%5B%5D=wbTjh28&channelIds%5B%5D=sycfxl&channelIds%5B%5D=cfTjh&channelIds%5B%5D=jgdhb&channelIds%5B%5D=tydhb&channelIds%5B%5D=jgxck&channelIds%5B%5D=jgyxl&channelIds%5B%5D=tyyxl&channelIds%5B%5D=dgBTAcp&channelIds%5B%5D=27637097&channelIds%5B%5D=27637101&channelIds%5B%5D=27637105&channelIds%5B%5D=27637109&channelIds%5B%5D=27637113&channelIds%5B%5D=27637117&channelIds%5B%5D=27637121&channelIds%5B%5D=27637125&channelIds%5B%5D=27637129&channelIds%5B%5D=27637133&channelIds%5B%5D=gyxj32&channelIds%5B%5D=yghxl&channelIds%5B%5D=ygcxl&channelIds%5B%5D=ygjxl&channelIds%5B%5D=ygbxl&channelIds%5B%5D=ygqxl&channelIds%5B%5D=yglxl&channelIds%5B%5D=ygzxl&channelIds%5B%5D=ygttg'
    headers = {"Accept": "text/html, */*; q=0.01", "Accept-Encoding": "gzip, deflate",
               "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", "Cache-Control": "no-cache", "Connection": "keep-alive",
               "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", "Host": "www.cebbank.com",
               "Origin": "http//www.cebbank.com", "Pragma": "no-cache",
               "Referer": "http//www.cebbank.com/site/gryw/yglc/lccp49/index.html",
               "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36",
               "X-Requested-With": "XMLHttpRequest"}
    response = session.post(url=url, data=get_data(1), headers=headers)
    resp_str = response.text.encode(response.encoding).decode('utf-8')
    soup = BeautifulSoup(resp_str, 'lxml')
    # 获取 《总页数》
    total_page = int(remove_space(soup.select('#totalpage')[0].text))

    rows = []
    for page in range(1, total_page + 1):
        response = session.post(url=url, data=get_data(1), headers=headers)
        time.sleep(SLEEP_SECOND)
        resp_str = response.text.encode(response.encoding).decode('utf-8')
        soup = BeautifulSoup(resp_str, 'html.parser')
        # 每一个产品使用被一个li标签包裹
        lis = soup.select('.lccp_main_content_tx')[0].select('li')
        # 遍历解析每一个产品
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
                except Exception:
                    row['href'] = ''
                    print(f'无法获取{row["cpbm"]}的产品详情页url')
                # 获取 产品名称
                try:
                    cpmc = a_tag['title'].replace(row['cpbm'], '').strip(')').strip('(')
                    if cpmc:
                        row['cpmc'] = cpmc
                except Exception:
                    print(f'无法获取{row["cpbm"]}的产品名称')
            # 解析 《业绩比较基准》
            try:
                lccp_syl = li.select('.lccp_syl')[0]
                title = remove_space(lccp_syl.select('.syl_title')[0].text)
                value = remove_space(lccp_syl.select('.lccp_ll')[0].text)
                yjbjjz = f'{title}:{value}' if value != '--' else ''
                if yjbjjz:
                    row['yjbjjz'] = yjbjjz
            except Exception as e:
                print(f'无法获取{row["cpbm"]}的业绩比较基准')

            # 解析 《投资期限\起点金额\风险等级》
            try:
                fls = li.select('.fl')
                for fl in fls:
                    text = remove_space(fl.text)
                    if '期限' in text:
                        tzqx = remove_space(fl.select('span')[0].text)
                        if tzqx:
                            row['tzqx'] = tzqx
                    elif '起点金额' in text:
                        qgje = remove_space(fl.select('span')[0].text)
                        if qgje:
                            row['qgje'] = qgje
                    elif '风险' in text:
                        if 'lc_fxdj' in fl['class']:
                            fxdj = remove_space(text)
                            if fxdj:
                                row['fxdj'] = fxdj
            except Exception as e:
                print(e)

            # 获取详情信息
            detail_url = row['href']
            detail_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache", "Connection": "keep-alive", "Host": "www.cebbank.com",
                "Pragma": "no-cache", "Referer": "http//www.cebbank.com/site/gryw/yglc/lccp49/index.html",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36"}
            detail_response = session.get(url=detail_url, headers=detail_headers)
            detail_row = {}

            detail_resp_str = detail_response.text.encode(detail_response.encoding).decode(
                'utf-8') if detail_response.encoding is not None else detail_response.text
            detail_soup = BeautifulSoup(detail_resp_str, 'lxml')
            lccp_xq_con_1 = detail_soup.select('.lccp_xq_con_1')[0]
            lccp_xq_con_2 = detail_soup.select('.lccp_xq_con_2')[0]
            lccp_xq_btm = detail_soup.select('.lccp_xq_btm')[0]
            fl_tags = lccp_xq_con_1.select('.fl')
            for fl_tag in fl_tags:
                fl_text = remove_space(fl_tag.text)
                if '业绩比较基准' in fl_text:
                    # 获取业绩比较基准的url链接
                    a_tags = fl_tag.select('a')
                    for a_tag in a_tags:
                        yjbjjz_url = remove_space(a_tag['href']) if check_attr(a_tag, 'href') and remove_space(
                            a_tag.text) == '基准图标' else ''
                        if yjbjjz_url:
                            detail_row['yjbjjz_url'] = yjbjjz_url
                    name = fl_tag.select('.syl_wz')
                    value = fl_tag.select('.syl_sz')
                    if name and value:
                        name_text = remove_space(name[0].text)
                        value_text = remove_space(value[0].text)
                        if name_text and value_text:
                            detail_row['yjbjjz'] = f'{name_text}:{value_text}'
                elif '理财期限' in fl_text:
                    value = remove_space(fl_tag.select('.lcqx_sz')[0].text)
                    if value:
                        detail_row['tzqx'] = value
                elif '起点金额' in fl_text:
                    value = remove_space(fl_tag.select('.qgje_sz')[0].text)
                    if value:
                        detail_row['qgje'] = value
                elif '风险等级' in fl_text:
                    value = remove_space(fl_tag.select('.qgje_sz')[0].text)
                    if value:
                        detail_row['fxdj'] = value
            name_tags = lccp_xq_con_2.select('.fdsy_con_name.fl')[0]
            value_tags = lccp_xq_con_2.select('.fdsy_con_nr.fl')[0]
            for name, value in zip(name_tags, value_tags):
                name_text = remove_space(name.text)
                value_text = remove_space(value.text)
                if name_text == '递增金额':
                    if value_text:
                        detail_row['dzje'] = value_text
                elif name_text == '开放日':
                    value_text = value_text if value_text != '--' else ''
                    if value_text:
                        detail_row['kfr'] = value_text
                elif name_text == '币种':
                    if value_text:
                        detail_row['mjbz'] = value_text
                elif name_text == '产品种类':
                    if value_text:
                        detail_row['zyms'] = value_text
            remain_account_tags = lccp_xq_btm.select('#remainAccount')
            if remain_account_tags:
                value = remove_space(remain_account_tags[0].text)
                if value:
                    detail_row['syed'] = value

            # 剩余额度            =================此处的请求无法获取到剩余额度===================
            cpInfo = ""
            if row['cpbm'].find("EB") != -1:
                cpInfo = row['cpbm'] + ",2,"
            else:
                cpInfo = row['cpbm'] + ",1,1001"
            fzcp1Info = ""
            fzcp2Info = ""
            iRandom = round(random.Random().random() * 100000000)
            syed_url = domain_url + "/eportalapply/jsp/lcys/lcys.jsp?cpInfo=" + cpInfo + "&fzcp1Info=" + fzcp1Info + "&fzcp2Info=" + fzcp2Info + "&random=" + str(
                iRandom)
            syed_headers = {"Accept": "text/html, */*; q=0.01", "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", "Cache-Control": "no-cache", "Connection": "keep-alive", "Content-Length": "0", "Cookie": "iss_cc=true; iss_svid=72239867d536d5ad1f92b52824455dcf; BIGipServerpool_waf_nport=!GeMhaoHch7/W7cHsBoHfVu2SmXZmubuBu8aBWC0IX5ig7YYqklN+WP90B8trxkfcm7Byu9FMg6VL; OUTFOX_SEARCH_USER_ID_NCOO=1199878561.7742903; iss_sid=15b0f2853c8e12ad8bbe790dd85fff7f; iss_nu=0; BIGipServerpool_nport=!irDaMwfR06IQxODsBoHfVu2SmXZmuRUaU08i8oitTVVSaScoZTLRk+pk5dfhVZ5HhJecNE70Qo0pMQ==; iss_id=dee37ecf4faa583e476f6332129c3a7f; iss_ot=1662619762658", "Host": "www.cebbank.com", "Origin": "http//www.cebbank.com", "Pragma": "no-cache", "Referer": "http//www.cebbank.com//site/gryw/yglc/lccpsj/27637101/22272964/index.html", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36", "X-Requested-With": "XMLHttpRequest"}
            syed_response: Response = session.get(url=syed_url, headers=syed_headers)
            time.sleep(SLEEP_SECOND)
            syed_res_str = remove_space(syed_response.text.encode(syed_response.encoding).decode(
                'utf-8') if syed_response.encoding else syed_response.text)
            syed_response = session.post(url=syed_res_str, headers=headers)
            time.sleep(SLEEP_SECOND)
            syed_res_str = remove_space(syed_response.text.encode(syed_response.encoding).decode(
                'utf-8') if syed_response.encoding else syed_response.text)
            # 产品状态
            foot_tags = detail_soup.select('.xq_foot')
            if foot_tags:
                foot_tag = foot_tags[0]
                cpzt_tags = foot_tag.select('.xq_btn.fl')
                if cpzt_tags:
                    cpzt = remove_space(cpzt_tags[0].text)
                    if cpzt:
                        detail_row['cpzt'] = cpzt

            # 产品公告
            cpgg_tag = soup.select('.cpgg')
            ggs = []
            if cpgg_tag:
                gg_link_tags = cpgg_tag[0].select('a')
                for gg_link_tag in gg_link_tags:
                    url = domain_url + gg_link_tag['href'] if check_attr(gg_link_tag, 'href') else ''
                    title = remove_space(gg_link_tag.text) if remove_space(gg_link_tag.text) != '更多 >>' else '更多'
                    if url and title:
                        ggs.append({
                            'url': url,
                            'title': title
                        })
            if ggs:
                row['ggs'] = json.dumps(ggs).encode().decode('unicode_escape')
            # 处理业绩比较基准url链接
            yjbjjz = {}
            yjbjjz_url = detail_row['yjbjjz_url']
            yjbjjz_headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache", "Connection": "keep-alive", "Host": "www.cebbank.com",
                "Pragma": "no-cache", "Referer": "http//www.cebbank.com/site/kfgn/tllssy/index.html",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"}
            yjbjjz_response = session.get(url=yjbjjz_url, headers=yjbjjz_headers)
            yjbjjz_response_str = yjbjjz_response.text.encode(yjbjjz_response.encoding).decode(
                'utf-8') if yjbjjz_response.encoding else yjbjjz_response.text
            yjbjjz_soup = BeautifulSoup(yjbjjz_response_str, 'html.parser')
            tr_tags = yjbjjz_soup.select('tr')
            col_names = []
            if tr_tags:
                col_names = [remove_space(p_tag.text) for p_tag in tr_tags[0].select('p')]
            col_values = []
            for tr_tag in tr_tags:
                if row['cpbm'] in remove_space(tr_tag.text):
                    # 解析这一行的数据
                    col_values = [remove_space(p_tag.text) for p_tag in tr_tag.select('p')]
                    break
            for col_name, col_value in zip(col_names, col_values):
                if col_name == '起息日':
                    yjbjjz['qxr'] = col_value
                elif col_name == '到期日':
                    yjbjjz['dqr'] = col_value
                elif col_name == '理财期限':
                    yjbjjz['lcqx'] = col_value
                elif col_name == '业绩比较基准':
                    yjbjjz['yjbjjz'] = col_value
            if yjbjjz:
                detail_row['yjbjjz'] = json.dumps(yjbjjz).encode().decode('unicode_escape')

            row.update(detail_row)
            del row['href']
            del row['yjbjjz_url']
            print(row)
            rows.append(row)
    return rows


if __name__ == '__main__':
    if '':
        print("==========")
