from bs4 import BeautifulSoup, Tag
from requests import Session

from utils.html_utils import check_attr
from utils.mark_log import insertLogToDB
from utils.selenium_utils import get_driver, close
from utils.spider_flow import SpiderFlow, process_flow
from utils.string_utils import remove_space

REQUEST_URL1 = 'https://wealth.cib.com.cn/retail/onsale/index.html'
REQUEST_URL2 = 'https://wealth.cib.com.cn/retail/onsale/DXLC.html'

TARGET_TABLE = 'ip_bank_xyyh_personal'
LOG_NAME = '兴业银行'


def parse_row(html_page, cols):
    soup = BeautifulSoup(html_page, 'lxml')
    table: Tag = soup.select('table')[0]
    trs = table.select('tr')
    rows = []
    for index, tr in enumerate(trs):
        if index == 0:
            continue
        else:
            tds = tr.select('td')
            row = []
            current_tr_index = 0
            # table一共有13列
            colspan = 1
            for row_index in range(cols):
                # 开始进入循环获取colspan
                if row_index == 0:
                    current_tr_index = 0
                    row.append(tds[row_index].text)
                    colspan = int(tds[row_index]['colspan']) if check_attr(tds[row_index], 'colspan') else 1
                else:
                    # 之后判断当前的colspan 如果>1表示 当前单元格与之前的共享单元格
                    if colspan > 1:
                        row.append(tds[current_tr_index].text)
                        colspan -= 1
                    else:
                        current_tr_index += 1
                        row.append(tds[current_tr_index].text)
                        colspan = int(tds[current_tr_index]['colspan']) if check_attr(tds[current_tr_index],
                                                                                      'colspan') else 1
        rows.append(row)
    return rows


def parse_table1(html_page,cols):
    rows = parse_row(html_page, cols)
    new_rows = []
    for row in rows:
        new_rows.append({
            'cpmc': row[0],
            'cpbm': row[1],
            'ipo_kssj': row[2],
            'ipo_jssj': row[3],
            'xsqy': row[4],
            'hblx': row[5],
            'cpqx': row[6],
            'cplx': row[7],
            'qgje': row[8],
            'fxdj': row[9],
            'yjbjjz': row[11],
            'cply': '兴业银行'
        })
    return new_rows


def parse_table2(html_page, cols: int):
    rows = parse_row(html_page, cols)
    new_rows = []
    for row in rows:
        new_rows.append({
            'cpmc': row[0],
            'cpbm': row[1],
            'ipo_kssj': row[2],
            'ipo_jssj': row[3],
            'xsqy': row[4],
            'hblx': row[5],
            'cpqx': row[6],
            'cplx': row[7],
            'qgje': row[8],
            'fxdj': row[9],
            'cply': '兴业银行'
        })
    return new_rows


class SpiderFlowImpl(SpiderFlow):
    def callback(self, conn, cursor, session: Session, log_id: int, **kwargs):
        driver = None
        try:
            driver = get_driver()
            driver.get(url=REQUEST_URL1)
            xy_owner = driver.execute_script("return document.documentElement.outerHTML")
            rows = parse_table1(xy_owner, 13)
            for row in rows:
                row['fxr'] = '兴业银行'
                row['logId'] = log_id
                insertLogToDB(cursor, row, TARGET_TABLE)
            driver.get(url=REQUEST_URL2)
            xy_agency = driver.execute_script("return document.documentElement.outerHTML")
            rows = parse_table2(xy_agency, 12)
            for row in rows:
                row['fxr'] = '代理销售'
                row['logId'] = log_id
                insertLogToDB(cursor, row, TARGET_TABLE)
        except Exception as e:
            raise e
        finally:
            close(driver)


if __name__ == '__main__':
    process_flow(LOG_NAME, TARGET_TABLE, SpiderFlowImpl())
