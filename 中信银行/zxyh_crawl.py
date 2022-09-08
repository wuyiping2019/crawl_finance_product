import json
import math
import time

from requests import Session

from utils.custom_exception import CustomException
from utils.db_utils import insert_to_db, check_table_exists, create_table, add_field, check_field_exists
from utils.mark_log import getLocalDate
from utils.spider_flow import SpiderFlow, process_flow

MARK_STR = 'zxyh'
TARGET_TABLE_PC = f'ip_bank_{MARK_STR}_pc'
TARGET_TABLE_MOBILE = f'ip_bank_{MARK_STR}_mobile'
PROCESSED_TABLE = f'ip_bank_{MARK_STR}'
LOG_NAME = '中信银行'
SEQUENCE_NAME = f'seq_{MARK_STR}'
TRIGGER_NAME = f'trigger_{MARK_STR}'
CRAWL_REQUEST_DETAIL_TABLE = f'crawl_{MARK_STR}'

TARGET_TABLE_PROCESSED = f'ip_bank_{MARK_STR}_processed'
STR_TYPE = 'clob'
NUMBER_TYPE = 'number(11)'
SLEEP_SECOND = 3


def process_data(prod_list: list, session):
    rows = []
    for prod in prod_list:
        row = {
            'cpmc': prod['PRD_NAME'],  # 产品名称
            'xsqd': prod['CHANNELS_NAME'],  # 销售渠道
            'jz': prod['NAV'],  # 净值
            'jzrq': prod['NAV_DATE'],  # 净值日期
            'cpbm': prod['PRD_CODE'],  # 产品编码
            'fhfs': prod['DIV_MODES_NAME'],  # 分红方式
            'fxdj': prod['RISK_LEVEL_NAME'],  # 风险等级
            'cpzt': prod['STATUS_NAME'],  # 产品状态
            'tzqx': prod['CYCLE_DAYS'],  # 投资期限
            'mjqsrq': prod['IPO_START_DATE'],  # 募集起始日期
            'mjjsrq': prod['IPO_END_DATE'],  # 募集结束日期
            'glr': prod['PRD_MANAGER_NAME'],  # 管理人
            'glrbm': prod['PRD_MANAGER'],  # 管理人编码
            'tgr': prod['PRD_TRUSTEE_NAME'],  # 托管人
            'tgrbm': prod['PRD_TRUSTEE'],  # 托管人编码
            'tabm': prod['TA_CODE'],  # TA编码
            'tamc': prod['TA_NAME']  # TA名称
        }
        # 获取产品的详情信息
        prod_info = fin_fund_detail_info(row['cpbm'], session)
        time.sleep(SLEEP_SECOND)
        prd_limit = query_limit_by_prdCode(row['cpbm'], session)
        time.sleep(SLEEP_SECOND)
        kline = fund_kline(row['cpbm'], session)
        time.sleep(SLEEP_SECOND)
        row.update(prod_info)
        row.update(prd_limit)
        row.update(kline)
        rows.append(row)
    return rows


def save_rows(cursor, rows: list, log_id: int):
    # 校验表是否存在
    if not check_table_exists(TARGET_TABLE_PROCESSED, cursor):
        # 创建表
        create_table(rows[0], STR_TYPE, NUMBER_TYPE, TARGET_TABLE_PROCESSED, cursor, SEQUENCE_NAME, TRIGGER_NAME)
    for row in rows:
        row['logId'] = log_id
        row['createTime'] = getLocalDate()
        try:
            insert_to_db(cursor, row, TARGET_TABLE_PROCESSED)
        except Exception as e:
            if isinstance(e, CustomException) and e.code == 1:
                # 表示是在插入oracle数据库的过程中出现插入不存在的字段的问题
                # 使用add_field方法添加字段
                for key in row.keys():
                    # 检查是否存在key
                    if not check_field_exists(key, TARGET_TABLE_PROCESSED, cursor):
                        # 如果不存在 则创建该字段
                        add_field(key, STR_TYPE, TARGET_TABLE_PROCESSED, cursor)
                # 重新插入数据
                insert_to_db(cursor, row, TARGET_TABLE_PROCESSED)
        cursor.connection.commit()


def fin_fund_product_list(cursor, log_id: int, session):
    url = 'https://m1.cmbc.com.cn/gw/m_app/FinFundProductList.do'
    headers = {"Accept": "application/json, text/plain, */*", "Accept-Encoding": "gzip, deflate, br",
               "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", "Connection": "keep-alive", "Content-Length": "377",
               "Content-Type": "application/json;charset=UTF-8",
               # "Cookie": "OUTFOX_SEARCH_USER_ID_NCOO=2040096739.6771917; bigipW_http_m1_cookie=!nYSShSamg1xL/+mCPONelVKPr9b9hCMHsYGACUKbVWf9c5us2lHA7GbrISAib/vv15IyhebF+tbO6e8=; org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE=zh_CN; BIGipServerUEB_tongyidianziqudao_app_41002_pool=!LZ1hPZU/gYjU5wg0lXP1ySZhZOpxgiT/TPyoEpXmjUVZE6Vq8pH17drAjrcUV2Bvlq8xCKA7ZFjJFxE=; BIGipServershoujiyinhang_geren_app_8000_pool=!cVtpjyeRF7pmcfU0lXP1ySZhZOpxgh24bT583iufzmqtveOrdoQBaWSS21duLXHrdTizmqHiGzjQNXs=; RSESSIONID=9648D5788E328E7EE2C6308BD566C149; lastAccessTime=1662341559965; JSESSIONID=NTYLRza6c8sLssOlnPcw8cON_sW4qoIQpXhiiStGLTAtt6vAx7C0!1605309896",
               "Host": "m1.cmbc.com.cn", "Origin": "https//m1.cmbc.com.cn",
               "Referer": "https//m1.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/fund-commission/prd-list",
               "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
               "sec-ch-ua-mobile": "?1", "sec-ch-ua-platform": "\"Android\"", "Sec-Fetch-Dest": "empty",
               "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
               "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36",
               "X-Tingyun": "c=B|MbVbeLGiVew;x=804e30a865b14a1d"}
    data = {
        "request": {
            "header": {
                "appId": "",
                "appVersion": "web",
                "appExt": "999",
                "device":
                    {
                        "appExt": "999",
                        "osType": "03",
                        "osVersion": "",
                        "uuid": ""
                    }
            },
            "body": {
                "pageSize": 10,
                "currentIndex": 0,
                "startId": 1,
                "prdTypeNameList": [],
                "sellingListTypeIndex": None,
                "sortFileldName": "LIQU_MODE_NAME",
                "sortFileldType": "DESC",
                "prdChara": "",
                "liveTime": "",
                "pfirstAmt": "",
                "currType": "",
                "keyWord": "",
                "isKJTSS": "0"
            }
        }
    }
    response = session.post(url=url, json=data, headers=headers)
    time.sleep(SLEEP_SECOND)
    loads = json.loads(response.text)
    # 数据总条数
    total_size = loads['response']['totalSize']
    # 获取的第一页的数据当做每页的数据条数
    page_size = data['request']['body']['pageSize']
    # 计算总页数
    page_count = math.ceil(total_size / page_size)
    # 处理第一页的数据
    rows = process_data(loads['response']['list'], session)
    # 保存第二页的数据
    save_rows(cursor, rows, log_id)
    # 从第二页开始遍历获取每页的数据并处理之后进行保存
    for page_index in range(2, page_count + 1):
        # 修改请求参数
        data['request']['body']['currentIndex'] = (page_index - 1) * 10
        data['request']['body']['startId'] = (page_index - 1) * 10 + 1
        # 请求该页的数据
        response = session.post(url=url, json=data, headers=headers)
        time.sleep(SLEEP_SECOND)
        loads = json.loads(response.text)
        # 处理响应数据
        rows = process_data(loads['response']['list'], session)
        # 保存响应数据
        save_rows(cursor, rows, log_id)


def fin_fund_detail_info(prd_code, session):
    url = 'https://m1.cmbc.com.cn/gw/m_app/FinFundDetailInfo.do'
    data = {
        "request":
            {"header":
                {
                    "appId": "",
                    "appVersion": "web",
                    "appExt": "999",
                    "device":
                        {
                            "appExt": "999",
                            "osType": "03",
                            "osVersion": "",
                            "uuid": ""
                        }
                },
                "body":
                    {
                        "prdCode": prd_code
                    }
            }
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        # "Content-Length": "167",
        "Content-Type": "application/json;charset=UTF-8",
        # "Cookie": "OUTFOX_SEARCH_USER_ID_NCOO=2040096739.6771917; bigipW_http_m1_cookie=!nYSShSamg1xL/+mCPONelVKPr9b9hCMHsYGACUKbVWf9c5us2lHA7GbrISAib/vv15IyhebF+tbO6e8=; org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE=zh_CN; BIGipServerUEB_tongyidianziqudao_app_41002_pool=!LZ1hPZU/gYjU5wg0lXP1ySZhZOpxgiT/TPyoEpXmjUVZE6Vq8pH17drAjrcUV2Bvlq8xCKA7ZFjJFxE=; BIGipServershoujiyinhang_geren_app_8000_pool=!cVtpjyeRF7pmcfU0lXP1ySZhZOpxgh24bT583iufzmqtveOrdoQBaWSS21duLXHrdTizmqHiGzjQNXs=; RSESSIONID=9648D5788E328E7EE2C6308BD566C149; JSESSIONID=NTYLRza6c8sLssOlnPcw8cON_sW4qoIQpXhiiStGLTAtt6vAx7C0!1605309896; lastAccessTime=1662341975290",
        "Host": "m1.cmbc.com.cn",
        "Origin": "https//m1.cmbc.com.cn",
        "Referer": "https//m1.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/fund-commission/details?isZhuanShu=false&ktype=4&newFlag=0&prdCode=2119187301A",
        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": "\"Android\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36",
        "X-Tingyun": "c=B|MbVbeLGiVew;x=05baa23fa063480f"
    }
    response = session.post(url=url, json=data, headers=headers)
    loads = json.loads(response.text)
    fund_detail = loads['response']['fundDetail']
    attrs = {}
    ggs = []  # 公告
    for file in fund_detail['fileLists']:
        if file['FILE_TYPE'] == '8':
            attrs['yjbjjzsm'] = json.dumps(
                {
                    'fbrq': file['ISS_DATE'],
                    'url': file['fileHref'],
                    'title': file['FILETYPENAME']
                }
            )
        elif file['FILE_TYPE'] == "5":
            ggs.append({'title': file['TITLE'], 'url': file['fileHref'], 'fbrq': file['ISS_DATE']})
        elif file['FILE_TYPE'] == "1":
            attrs['dzht'] = json.dumps(
                {
                    'fbrq': file['ISS_DATE'],
                    'url': file['fileHref'],
                    'title': file['FILETYPENAME']
                }
            ).encode().decode('unicode_escape')
        elif file['FILE_TYPE'] == "0":
            attrs['fxjss'] = json.dumps(
                {
                    'fbrq': file['ISS_DATE'],
                    'url': file['fileHref'],
                    'title': file['FILETYPENAME']
                }
            ).encode().decode('unicode_escape')
    attrs['ggs'] = json.dumps(ggs).encode().decode('unicode_escape')
    row = {
        'jz': fund_detail['NAV'],  # 净值
        'jzrq': fund_detail['NAV_DATE'],  # 净值日期
        # 'fxj': fund_detail['ISS_PRICE'],  # 发行价       --------------不确定-----------
        'scgmqd': str(fund_detail['PFIRST_AMT']) + '元',  # 首次购买起点

        'qgje': str(fund_detail['PFIRST_AMT']) + '元',  # 起购金额

        'zjgmqd': str(fund_detail['PAPP_AMT']) + '元',  # 追加购买起点
        'zxrgdw': str(fund_detail['PSUB_UNIT']) + '元',  # 最小认购单位
        'shqdfe': str(fund_detail['PMIN_RED']) + '份',  # 赎回起点份额
        'zdcyfe': str(fund_detail['PMIN_HOLD']) + '份',  # 最低持有份额
        'zxshdw': str(fund_detail['PRED_SUB_UNIT']) + '份',  # 最小赎回单位
        'fxdj': str(fund_detail['RISK_LEVEL_NAME']),  # 风险等级
        'cpzt': str(fund_detail['STATUS_NAME']),  # 产品状态
        'clrq': str(fund_detail['ES_TAB_DATE']),  # 成立日期
        'yjbjjz': str(fund_detail['LIQU_MODE_NAME']),  # 业绩比较基准
        'sgshrq': json.dumps(
            {
                'kfr': fund_detail['dateLineMap']['openDay'],  # 开放日 可以申购该产品
                'xygkfr': fund_detail['dateLineMap']['nextOpenDay'],  # 下一个开放日
                'kfrqrr': fund_detail['dateLineMap']['openConfirmDay'],  # 开放日确认日
                'xygkfrqrr': fund_detail['dateLineMap']['nextOpenConfirmDay'],  # 下一个开放日确认日
                'shr': fund_detail['dateLineMap']['openReddemDay'],  # 赎回日
                'xygshr': fund_detail['dateLineMap']['nextOpenReddemDay']  # 下一个赎回日
            }
        ),
        'shsj': str(fund_detail['RED_DAYS']) + '天',
        'mjbz': str(fund_detail['CURR_TYPE_NAME'])
    }
    row.update(attrs)
    return row


def query_limit_by_prdCode(prd_code, session):
    url = 'https://m1.cmbc.com.cn/gw/m_app/QueryLimitByPrdCode.do'
    data = {
        "request": {
            "header": {
                "appId": "",
                "appVersion": "web",
                "appExt": "999",
                "device":
                    {
                        "appExt": "999",
                        "osType": "03",
                        "osVersion": "",
                        "uuid": ""
                    }
            },
            "body": {
                "prdCode": prd_code
            }
        }
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        # "Content-Length": "167",
        "Content-Type": "application/json;charset=UTF-8",
        # "Cookie": "OUTFOX_SEARCH_USER_ID_NCOO=2040096739.6771917; bigipW_http_m1_cookie=!nYSShSamg1xL/+mCPONelVKPr9b9hCMHsYGACUKbVWf9c5us2lHA7GbrISAib/vv15IyhebF+tbO6e8=; org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE=zh_CN; BIGipServerUEB_tongyidianziqudao_app_41002_pool=!LZ1hPZU/gYjU5wg0lXP1ySZhZOpxgiT/TPyoEpXmjUVZE6Vq8pH17drAjrcUV2Bvlq8xCKA7ZFjJFxE=; BIGipServershoujiyinhang_geren_app_8000_pool=!cVtpjyeRF7pmcfU0lXP1ySZhZOpxgh24bT583iufzmqtveOrdoQBaWSS21duLXHrdTizmqHiGzjQNXs=; JSESSIONID=svMMb5h_PtRO-JL12PPV7bRi2KjFxT6-lbcsN6hZHRDLaBgWcplh!1605309896; RSESSIONID=A352F2004FF78FD554B12EBA5394C0BC; lastAccessTime=1662361583542",
        "Host": "m1.cmbc.com.cn",
        "Origin": "https//m1.cmbc.com.cn",
        "Pragma": "no-cache",
        "Referer": "https//m1.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/fund-commission/details?isZhuanShu=false&ktype=4&newFlag=0&prdCode=2119187301A",
        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": "\"Android\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36",
        "X-Tingyun": "c=B|MbVbeLGiVew;x=1e4bf920b1504660"
    }

    response = session.post(url=url, json=data, headers=headers)
    loads = json.loads(response.text)
    return {'syed': loads['response']['resultMap']['TotSaleAmt']}


def fund_kline(prd_code, session):
    url = 'https://m1.cmbc.com.cn/gw/m_app/FundKline.do'
    data = {
        "request":
            {
                "header": {
                    "appId": "",
                    "appVersion": "web",
                    "appExt": "999",
                    "device": {
                        "appExt": "999",
                        "osType": "03",
                        "osVersion": "", "uuid": ""
                    }
                },
                "body": {
                    "prdCode": prd_code,
                    "timeScope": "1"
                }
            }
    }
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        # "Content-Length": "183",
        "Content-Type": "application/json;charset=UTF-8",
        # "Cookie": "OUTFOX_SEARCH_USER_ID_NCOO=2040096739.6771917; bigipW_http_m1_cookie=!nYSShSamg1xL/+mCPONelVKPr9b9hCMHsYGACUKbVWf9c5us2lHA7GbrISAib/vv15IyhebF+tbO6e8=; org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE=zh_CN; BIGipServerUEB_tongyidianziqudao_app_41002_pool=!LZ1hPZU/gYjU5wg0lXP1ySZhZOpxgiT/TPyoEpXmjUVZE6Vq8pH17drAjrcUV2Bvlq8xCKA7ZFjJFxE=; BIGipServershoujiyinhang_geren_app_8000_pool=!cVtpjyeRF7pmcfU0lXP1ySZhZOpxgh24bT583iufzmqtveOrdoQBaWSS21duLXHrdTizmqHiGzjQNXs=; JSESSIONID=svMMb5h_PtRO-JL12PPV7bRi2KjFxT6-lbcsN6hZHRDLaBgWcplh!1605309896; RSESSIONID=A352F2004FF78FD554B12EBA5394C0BC; lastAccessTime=1662361586386",
        "Host": "m1.cmbc.com.cn",
        "Origin": "https//m1.cmbc.com.cn",
        "Pragma": "no-cache",
        "Referer": "https//m1.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/fund-commission/details?isZhuanShu=false&ktype=4&newFlag=0&prdCode=2119187301A",
        "sec-ch-ua": "\"Google Chrome\";v=\"95\", \"Chromium\";v=\"95\", \";Not A Brand\";v=\"99\"",
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": "\"Android\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36",
        "X-Tingyun": "c=B|MbVbeLGiVew;x=7401b2ee5b394b71"
    }
    response = session.post(url=url, json=data, headers=headers)
    loads = json.loads(response.text)
    jz_list = []
    for row in loads['response']['resultMap']['list']:
        jz_list.append(
            {
                'jzrq': row['ISS_DATE'],
                'jz': row['NAV'],
                'ljjz': row['TOT_NAV']

            }
        )
    return {'lsjz': json.dumps(jz_list).encode().decode('unicode_escape')}


class SpiderFlowImpl(SpiderFlow):
    def callback(self, conn, cursor, session: Session, log_id: int, **kwargs):
        fin_fund_product_list(cursor, log_id, session)


if __name__ == '__main__':
    process_flow(LOG_NAME, TARGET_TABLE_PROCESSED, SpiderFlowImpl())
