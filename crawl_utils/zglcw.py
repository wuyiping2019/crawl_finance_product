# 解析中国理财网中产品的数据
from bs4 import BeautifulSoup, Tag
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By

from scrapy_modules.custome_exceptions import ZGLCWTableParseException
from crawl_utils.string_utils import remove_space

ZGLCW_REQUEST_URL = 'https://www.chinawealth.com.cn/zzlc/jsp/lccp.jsp'


def parse_zglcw_table(html_str: str):
    soup = BeautifulSoup(html_str, 'lxml')
    product_detail = soup.select('#productDetailLeft')[0]
    h5 = product_detail.select('h5')[0]
    cpmc = remove_space(h5.select('em')[0].text)
    cpzt = remove_space(h5.select('span')[0].text)
    lis = product_detail.select('li')
    if len(lis) % 2 != 0:
        raise ZGLCWTableParseException
    row = {}
    key: Tag = None
    value: Tag = None
    for li in lis:
        if not (key and value):
            row[key.text] = value
            key, value = None, None
        if key is None:
            key = li
        if key and value is None:
            value = li

    transformKey = {}
    for k, v in row.items():
        if k[:-1] == '登记编码':
            transformKey['djbm'] = v.text
        elif k[:-1] == '发行机构':
            transformKey['fxjg'] = v.text
        elif k[:-1] == '运作模式':
            transformKey['yzms'] = v.text
        elif k[:-1] == '募集方式':
            transformKey['mjfs'] = v.text
        elif k[:-1] == '期限类型':
            transformKey['qxlx'] = v.text
        elif k[:-1] == '募集币种':
            transformKey['mjbz'] = v.text
        elif k[:-1] == '投资性质':
            transformKey['tzxz'] = v.text
        elif k[:-1] == '风险等级':
            transformKey['fxdj'] = v.text
        elif k[:-1] == '募集起始日期':
            transformKey['mjqsrq'] = v.text
        elif k[:-1] == '募集结束日期':
            transformKey['mjjsrq'] = v.text
        elif k[:-1] == '产品起始日期':
            transformKey['cpqsrq'] = v.text
        elif k[:-1] == '产品结束日期':
            transformKey['cpjsrq'] = v.text
        elif k[:-1] == '业务起始日':
            transformKey['ywqsr'] = v.text
        elif k[:-1] == '业务结束日':
            transformKey['ywjsr'] = v.text
        elif k[:-1] == '实际天数':
            transformKey['sjts'] = v.text
        elif k[:-1] == '初始净值':
            transformKey['csjz'] = v.text
        elif k[:-1] == '产品净值':
            transformKey['cpjz'] = v.text
        elif k[:-1] == '累计净值':
            transformKey['ljjz'] = v.text
        elif k[:-1] == '最近一次兑付收益率':
            transformKey['zjycdfsyl'] = v.text
        elif k[:-1] == '产品特殊属性':
            transformKey['cptssx'] = v.text
        elif k[:-1] == '产品特殊属性':
            transformKey['cptssx'] = v.text
        elif k.find('投资资产类型') != -1:
            transformKey['tzzclx'] = v.text
        elif k.find('代销机构') != -1:
            transformKey['dxjg'] = v.text
        elif k.find('预期最低收益率') != -1:
            transformKey['yqzdsyl'] = v.text
        elif k.find('预期最高收益率') != -1:
            transformKey['yqzgsyl'] = v.text
        elif k.find('业绩比较基准') != -1:
            transformKey['yjbjjz'] = ';'.join([div.text for div in v.select('div')])
        elif k.find('销售区域') != -1:
            transformKey['xsqy'] = v.text
    transformKey['cpmc'] = cpmc
    transformKey['cpzt'] = cpzt
    return transformKey


def updateZGLCWInfo(cursor, target_table, properties: dict):
    temp_str = ''
    for k, v in properties.items():
        if k in ['logId', 'cpbm']:
            continue
        else:
            temp_str += f'{k} = "{v}",'
    temp_str.strip(',')
    sql = f"""
        update {target_table} 
        set  {temp_str}
        where logId = {properties['logId']} and cpbm = {properties['cpbm']}
    """
    cursor.execute(sql)