import json
import time
from bs4 import BeautifulSoup
from utils.html_utils import check_attr
from utils.string_utils import remove_space
from zggdyh_config import SLEEP_SECOND


def process_yqsy_type(session):
    """
    处理
    url = http://www.cebbank.com/site/gryw/yglc/lccp49/index.html
    网址中收益类型为《预期收益》的数据
    :return:
    """
    domain_url = 'http://www.cebbank.com'
    url = domain_url + '/eportal/ui?moduleId=12073&struts.portlet.action=/app/yglcAction!listProduct.action'
    get_data = lambda page: \
        [
            ('codeOrName', ''),
            ('TZBZMC', ''),
            ('sylxArr[]', '00'),
            ('SFZS', ''),
            ('qxrUp', 'Y'),
            ('qxrDown', ''),
            ('dqrUp', ''),
            ('dqrDown', ''),
            ('qdjeUp', ''),
            ('qdjeDown', ''),
            ('qxUp', ''),
            ('qxDown', ''),
            ('yqnhsylUp', ''),
            ('yqnhsylDown', ''),
            ('page', f'{page}'),
            ('pageSize', '12'),
            ('channelIds[]', 'yxl94'),
            ('channelIds[]', 'ygelc79'),
            ('channelIds[]', 'hqb30'),
            ('channelIds[]', 'dhb2'),
            ('channelIds[]', 'cjh'),
            ('channelIds[]', 'gylc70'),
            ('channelIds[]', 'Ajh67'),
            ('channelIds[]', 'Ajh84'),
            ('channelIds[]', '901776'),
            ('channelIds[]', 'Bjh91'),
            ('channelIds[]', 'Ejh99'),
            ('channelIds[]', 'Tjh70'),
            ('channelIds[]', 'tcjh96'),
            ('channelIds[]', 'ts43'),
            ('channelIds[]', 'ygjylhzhMOM25'),
            ('channelIds[]', 'yxyg87'),
            ('channelIds[]', 'zcpzjh64'),
            ('channelIds[]', 'wjyh1'),
            ('channelIds[]', 'smjjb9'),
            ('channelIds[]', 'ty90'),
            ('channelIds[]', 'tx16'),
            ('channelIds[]', 'ghjx6'),
            ('channelIds[]', 'ygxgt59'),
            ('channelIds[]', 'wbtcjh3'),
            ('channelIds[]', 'wbBjh77'),
            ('channelIds[]', 'wbTjh28'),
            ('channelIds[]', 'sycfxl'),
            ('channelIds[]', 'cfTjh'),
            ('channelIds[]', 'jgdhb'),
            ('channelIds[]', 'tydhb'),
            ('channelIds[]', 'jgxck'),
            ('channelIds[]', 'jgyxl'),
            ('channelIds[]', 'tyyxl'),
            ('channelIds[]', 'dgBTAcp'),
            ('channelIds[]', '27637097'),
            ('channelIds[]', '27637101'),
            ('channelIds[]', '27637105'),
            ('channelIds[]', '27637109'),
            ('channelIds[]', '27637113'),
            ('channelIds[]', '27637117'),
            ('channelIds[]', '27637121'),
            ('channelIds[]', '27637125'),
            ('channelIds[]', '27637129'),
            ('channelIds[]', '27637133'),
            ('channelIds[]', 'gyxj32'),
            ('channelIds[]', 'yghxl'),
            ('channelIds[]', 'ygcxl'),
            ('channelIds[]', 'ygjxl'),
            ('channelIds[]', 'ygbxl'),
            ('channelIds[]', 'ygqxl'),
            ('channelIds[]', 'yglxl'),
            ('channelIds[]', 'ygzxl'),
            ('channelIds[]', 'ygttg')
        ]
    headers = {"Accept": "text/html, */*; q=0.01", "Accept-Encoding": "gzip, deflate",
               "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", "Cache-Control": "no-cache", "Connection": "keep-alive",
               "Content-Length": "1423", "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
               "Host": "www.cebbank.com", "Origin": "http//www.cebbank.com", "Pragma": "no-cache",
               "Referer": "http//www.cebbank.com/site/gryw/yglc/lccp49/index.html",
               "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
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
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        soup = BeautifulSoup(resp_str, 'lxml')
        # 每一个li代表一条数据
        lis = soup.select('.lccp_main_content_tx')[0].select('li')
        # 针对li中的列对数据进行解析
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

            # 解析 《预期年化收益率》
            try:
                lccp_syl = li.select('.lccp_syl')[0]
                name = remove_space(lccp_syl.select('.syl_title')[0].text)
                value = remove_space(lccp_syl.select('.lccp_ll')[0].text)
                if name and value:
                    row['yjbjjz'] = f'{name}:{value}'
            except Exception as e:
                print(f'无法获取{row["cpbm"]}的业绩比较基准')

            # 解析 《期限、起点金额》
            fls = li.select('.fl')
            for fl in fls:
                text = remove_space(fl.text)
                if "期限" in text:
                    try:
                        tzqx = remove_space(fl.select('span')[0].text)
                        if tzqx:
                            row['tzqx'] = tzqx
                    except Exception as e:
                        print(f'无法获取{row["cpbm"]}的投资期限')
                if '起点金额' in text:
                    try:
                        qgje = remove_space(fl.select('span')[0].text)
                        if qgje:
                            row['qgje'] = qgje
                    except Exception as e:
                        print(f'无法获取{row["cpbm"]}的起点金额')
            # 解析产品的《风险等级》
            try:
                fxdj = remove_space(li.select('.lc_fxdj')[0].text)
                if fxdj:
                    row['fxdj'] = fxdj
            except Exception as e:
                print(f'无法获取{row["cpbm"]}的风险等级')
            # =================================================
            # 进一步进入详情页进行解析
            detail_row = {}
            'http://www.cebbank.com/site/gryw/yglc/lccpsj/Tjh70/941246/index.html'
            detail_url = row['href']
            if not detail_url:
                continue
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                "Accept-Encoding": "gzip, deflate", "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Cache-Control": "no-cache", "Connection": "keep-alive", "Host": "www.cebbank.com",
                "Pragma": "no-cache", "Referer": "http//www.cebbank.com/site/gryw/yglc/lccp49/index.html",
                "Upgrade-Insecure-Requests": "1",
                "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36"}
            detail_response = session.get(url=detail_url, headers=headers)
            time.sleep(SLEEP_SECOND)
            detail_resp_str = detail_response.text.encode(detail_response.encoding).decode('utf-8')
            detail_soup = BeautifulSoup(detail_resp_str, 'lxml')
            lccp_xq_con_1 = detail_soup.select('.lccp_xq_con_1')[0]
            lccp_xq_con_2 = detail_soup.select('.lccp_xq_con_2')[0]
            # 获取业绩比较基准
            try:
                title = remove_space(lccp_xq_con_1.select('.xq_syl.fl')[0].select('.syl_wz')[0].text)
                value = remove_space(lccp_xq_con_1.select('.xq_syl.fl')[0].select('.syl_sz')[0].text)
                yjbjjz = f'{title}:{value}'
                if title and value:
                    detail_row['yjbjjz'] = yjbjjz
            except Exception as e:
                print(f'无法获取{row["cpbm"]}的业绩比较基准')
            # 获取投资期限
            try:
                title = remove_space(lccp_xq_con_1.select('.xq_lcqx.fl')[0].select('.lcqx_wz')[0].text)
                value = remove_space(lccp_xq_con_1.select('.xq_lcqx.fl')[0].select('.lcqx_sz')[0].text)
                tzqx = value
                if tzqx:
                    detail_row['tzqx'] = tzqx
            except Exception as e:
                print(f'无法获取{row["cpbm"]}的投资期限')
            # 获取 起购金额
            try:
                title = remove_space(lccp_xq_con_1.select('.xq_qgje.fl')[0].select('.qgje_wz')[0].text)
                value = remove_space(lccp_xq_con_1.select('.xq_qgje.fl')[0].select('.qgje_sz')[0].text)
                qgje = value
                if qgje:
                    detail_row['qgje'] = qgje
            except Exception as e:
                print(f'无法获取{row["cpbm"]}的起购金额')
            # 获取 风险等级
            try:
                title = remove_space(lccp_xq_con_1.select('.xq_fxdj.fl')[0].select('.qgje_wz')[0].text)
                value = remove_space(lccp_xq_con_1.select('.xq_fxdj.fl')[0].select('.qgje_sz')[0].text)
                fxdj = value
                if fxdj:
                    detail_row['fxdj'] = fxdj
            except Exception as e:
                print(f'无法获取{row["cpbm"]}的风险等级')

            try:
                name_lis = detail_soup.select('.fdsy_con_name.fl')[0].select('li')
                value_lis = detail_soup.select('.fdsy_con_nr.fl')[0].select('li')
                for name, value in zip(name_lis, value_lis):
                    name_text = remove_space(name.text)
                    value_text = remove_space(value.text)
                    if '递增金额' in name_text:
                        if value_text:
                            detail_row['dzje'] = value_text
                    elif '开放日' in name_text:
                        value_text = value_text if value_text != '--' else ''
                        if value_text:
                            detail_row['kfr'] = value_text
                    elif '币种' in name_text:
                        if value_text:
                            detail_row['mjbz'] = value_text
                    elif '产品种类' in name_text:
                        if value_text:
                            detail_row['sylx'] = value_text
            except Exception as e:
                print(f'无法获取{row["cpbm"]}的递增金额、开放日、币种、产品种类')

            # 获取剩余额度
            try:
                syed = remove_space(detail_soup.select('#remainAccount')[0].text)
                if syed:
                    detail_row['syed'] = syed
            except Exception as e:
                print(f'无法获取{row["cpbm"]}的剩余额度')
            # 获取 产品状态
            try:
                a_tags = detail_soup.select('.xq_foot')[0].select('a')
                for a_tag in a_tags:
                    if check_attr(a_tag, 'data-analytics-click'):
                        cpzt = remove_space(a_tag.text)
                        if cpzt:
                            detail_row['cpzt'] = cpzt
            except Exception as e:
                print(f'无法获取{row["cpbm"]}的产品状态')
            # 获取产品的产品公告
            try:
                ggs = []
                gg_tags = detail_soup.select('.cpgg')[0].select('a')
                for gg in gg_tags:
                    gg_href = 'http://www.cebbank.com' + gg['href']
                    gg_title = remove_space(gg.text)
                    gg_title = gg_title if gg_title != '更多>>' else '更多'
                    ggs.append(
                        {
                            'url': gg_href,
                            'title': gg_title
                        }
                    )
                if ggs:
                    detail_row['ggs'] = json.dumps(ggs).encode().decode('unicode_escape')
            except Exception as e:
                print(f'无法获取{row["cpbm"]}的公告')
            # 获取产品说明书
            row.update(detail_row)
            del row['href']
            print(row)
            rows.append(row)
    return rows
