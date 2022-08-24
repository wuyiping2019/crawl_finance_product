import requests
from bs4 import BeautifulSoup
from requests import Session
import re
from utils.mark_log import insertLogToDB
from utils.selenium_utils import get_driver, close
from utils.spider_flow import process_flow, SpiderFlow

REQUEST_URL = 'http://www.cgbchina.com.cn/Channel/24530070'
LOG_NAME = '广发银行'
TARGET_TABLE = 'ip_bank_gfyh_personal'


def get_pages(driver):
    page = driver.execute_script('return document.documentElement.outerHTML')
    soup = BeautifulSoup(page, 'lxml')
    select = soup.select('body > div.main > div.finProduct > div > div > span:nth-child(5)')[0].text.split('/')[1]
    return int(select.replace('页', ''))


def parse_page(page):
    rows = []
    soup = BeautifulSoup(page, 'lxml')
    trs = soup.select('tr')
    for index, tr in enumerate(trs):
        row = {}
        tds = tr.select('td')
        if index == 0:
            continue
        else:
            row['cplx'] = re.sub('\s', '', tds[0].text)
            row['cpmc'] = re.sub('\s', '', tds[1].text)
            row['cpbm'] = re.sub('\s', '', tds[2].text)
            row['fxjg'] = re.sub('\s', '', tds[3].text)
            row['cply'] = re.sub('\s', '', tds[4].text)
            row['fxdj'] = re.sub('\s', '', tds[5].text)
            row['szkh'] = re.sub('\s', '', tds[6].text)
            row['qgje'] = re.sub('\s', '', tds[7].text)
            row['sfbz'] = re.sub('\s', '', tds[8].text)
            row['sffs'] = re.sub('\s', '', tds[9].text)
            row['cpqx'] = re.sub('\s', '', tds[10].text)
            rows.append(row)
    return rows


class SpiderFlowImpl(SpiderFlow):
    def callback(self, conn, cursor, session: Session, log_id: int, **kwargs):
        driver = None
        try:
            driver = get_driver()
            driver.get(REQUEST_URL)
            script = driver.execute_script('return document.documentElement.outerHTML')
            # 获取总页数
            pages = get_pages(driver)
            # 遍历获取各页
            for page in range(1, pages + 1, 1):
                driver.execute_script('gotoPage(%s)' % page)
                currentPage = driver.execute_script('return document.documentElement.outerHTML')
                rows = parse_page(currentPage)
                for row in rows:
                    insertLogToDB(cur=cursor, properties=row, log_table=TARGET_TABLE)
        except Exception as e:
            raise e
        finally:
            close(driver)


if __name__ == '__main__':
    process_flow(log_name=LOG_NAME, target_table=TARGET_TABLE, callback=SpiderFlowImpl())
