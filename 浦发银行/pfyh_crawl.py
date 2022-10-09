import json
from utils.db_utils import init_oracle, get_conn_oracle, close
from utils.mark_log import insertLogToDB, mark_start_log, mark_success_log, mark_failure_log, getLocalDate, \
    get_generated_log_id, updateLogToDB, get_write_count
from utils.global_config import DB_ENV
from requests import Session
from utils.db_utils import select_from_db, update_else_insert_to_db
from utils.spider_flow import SpiderFlow, process_flow
import urllib3
urllib3.disable_warnings()

import time
import requests

URL = r'https://per.spdb.com.cn/was5/web/search'
NAME = '浦发银行'
TARGET_TABLE = 'ip_bank_spdb_personal'
if DB_ENV == 'ORACLE':
    init_oracle()

headers = {
    'X-Requested-With': 'XMLHttpRequest',
    'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'
}

HEADERS_MOBILE = {"Accept": "application/json", "Accept-Encoding": "gzip, deflate, br",
                  "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                  "Cache-Control": "no-cache", "Connection": "keep-alive",
                  "Content-Type": "application/json",
                  # "Cookie": "mobwapGsessionid=151DD8BA8388611ED934AB3D5B29BAC4D; mobwapLbanGsessionid=0BFA0023E38AF11ED9DDB295B4ED64495; msJsessionid=4C4EB205BDDE94ACB318050EECD70705",
                  # "Host": "wap.spdb.com.cn", "Origin": "https//wap.spdb.com.cn",
                  "Pragma": "no-cache",
                  "Referer": "https//wap.spdb.com.cn/mspmk-cli-financechannel/financeCommonList?H5Channel=400",
                  "sec-ch-ua": "\"Microsoft Edge\";v=\"105\", \" Not;A Brand\";v=\"99\", \"Chromium\";v=\"105\"",
                  "sec-ch-ua-mobile": "?1", "sec-ch-ua-platform": "\"Android\"",
                  "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                  "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Mobile Safari/537.36 Edg/105.0.1343.42"}

TYPES_MOBILE = [
    {
        'url': 'https://wap.spdb.com.cn/mspmk-web-financechannel/QueryFinanceList.hh',
        'method': 'post',
        'requestParams': {"ReqHeader": {"AlgoType": "1", "MarketingSession": "\"\"",
                                        "DeviceInfo": "WEB_MAPPING_deviceID|iPhone|iPhoneWEB_MAPPING_buildType",
                                        "DeviceDigest": "WEB_MAPPING_getDeviceDigest", "NetWorkType": "1",
                                        "NetWorkName": "WEB_MAPPING_getNetWorkName", "AppVersion": "",
                                        "AppDeviceType": "WEB_MAPPING_deviceBrand WEB_MAPPING_deviceModel",
                                        "AppExVersion": ""},
                          "ReqBody": {"SegmentFlag": "0", "BeginNumber": "0", "QueryNumber": "10", "MobileNo": "",
                                      "Finance_SelectType": "", "Finance_Sort": "0", "Finance_SortFlag": "0",
                                      "flag": "viewAppearState"}}
    }
]

MOBILE_DETAIL_URL = 'https://wap.spdb.com.cn/mspmk-web-finance/QueryFinanceProduct.hh'
MOBILE_DETAIL_METHOD = 'post'
HEADERS_MOBILE_DETAIL = {"Accept": "application/json", "Accept-Encoding": "gzip, deflate, br",
                         "Accept-Language": "zh-CN,zh;q=0.9", "Cache-Control": "no-cache", "Connection": "keep-alive",
                         "Content-Type": "application/json; charset=UTF-8",
                         "Cookie": "mobwapGsessionid=18845A429388311EDB93195319C98A94A; mobwapLbanGsessionid=1FCEAF0CA388311EDB799B9725AADA930; msJsessionid=24C12AF771698544693F496713EA44B5",
                         "Pragma": "no-cache",
                         "Referer": "https//wap.spdb.com.cn/mspmk-cli-financebuy/productDetails?FinanceNo=2301222290&H5Channel=400&FromUrl=%2Fmspmk-cli-financechannel%2FfinanceCommonList%3FH5Channel%3D400",
                         "sec-ch-ua": "\"Google Chrome\";v=\"105\", \"Not)A;Brand\";v=\"8\", \"Chromium\";v=\"105\"",
                         "sec-ch-ua-mobile": "?1", "sec-ch-ua-platform": "\"Android\"", "Sec-Fetch-Dest": "empty",
                         "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
                         "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Mobile Safari/537.36"
                         }
PARAMS_MOBILE_DETAIL = {
            "ReqHeader": {
                "AlgoType": "1",
                "MarketingSession": "\"\"",
                "DeviceInfo": "WEB_MAPPING_deviceID|iPhone|iPhoneWEB_MAPPING_buildType",
                "DeviceDigest": "WEB_MAPPING_getDeviceDigest",
                "NetWorkType": "1",
                "NetWorkName": "WEB_MAPPING_getNetWorkName",
                "AppVersion": "",
                "AppDeviceType": "WEB_MAPPING_deviceBrand WEB_MAPPING_deviceModel",
                "AppExVersion": ""
                },
            "ReqBody": {
                "FinanceNo": "",
                "MobileNo": ""
                }
            }


def process_spider(conn, cur, sess, log_id, **kwargs):
    types = [
        {
            'type': '固定期限',
            'searchword': "(product_type=3)*finance_limittime = %*%*(finance_state='可购买')"
        },
        {
            'type': '现金管理类',
            'searchword': "(product_type=4)*finance_limittime = %*%*(finance_state='可购买')"
        },
        {
            'type': '净值类',
            'searchword': "(product_type=2)*finance_limittime = %*%*(finance_state='可购买')"
        },
        {
            'type': '私行专属',
            'searchword': "(product_type=0)*finance_limittime = %*%*(finance_state='可购买')"
        },
        {
            'type': '专属产品',
            'searchword': "(product_type=1)*finance_limittime = %*%*(finance_state='可购买')"
        }
    ]
    process_details(sess, cur, log_id, types)


def process_details(sess, cur, log_id, types):
    """
    处理固定期限的理财数据
    :param sess:
    :param cur:
    :param log_id:
    :return:
    """
    for type in types:
        typeName = type['type']
        searchword = type['searchword']
        data = {
            'metadata': 'finance_state | finance_no | finance_allname | finance_anticipate_rate | finance_limittime | finance_lmttime_info | finance_type | docpuburl | finance_ipo_enddate | finance_indi_ipominamnt | finance_indi_applminamnt | finance_risklevel | product_attr | finance_ipoapp_flag | finance_next_openday',
            'channelid': '266906',
            'page': '1',
            'searchword': searchword
        }
        resp = sess.post(URL, data=data, headers=headers).text
        try:
            resp_dict = json.loads(resp)
        except Exception as e:
            print('请求结果无法解析为json', e, ' 忽略该异常')
            continue
        pageTotal = resp_dict['pageTotal']
        total = resp_dict['total']
        # 循环处理每一页的数据
        for page in range(1, pageTotal + 1, 1):
            data['page'] = str(page)
            response = sess.post(URL, data=data, headers=headers)
            status_code = response.status_code
            if status_code == 200:
                # 当正常请求时，但是结果无法转成对象，忽略异常（没有数据）
                rows = []
                try:
                    rows = json.loads(response.text)['rows']
                except Exception as e:
                    print('请求结果无法解析为json', e, ' 忽略该异常')
                for row in rows:
                    row = {k: str(v).replace('\'', '') for k, v in row.items()}
                    row = {
                        'cpzt': row['finance_state '],
                        'cpsms': row[' product_attr '],
                        'yqnhsyl': row[' finance_anticipate_rate '],
                        'finance_limittime': row[' finance_limittime '],
                        'finance_type': row[' finance_type '],
                        'tzqx': row[' finance_lmttime_info '],
                        'cpmc': row[' finance_allname '],
                        'scrgxx': row[' finance_indi_applminamnt '],
                        'docpuburl': row[' docpuburl '],
                        'qgje': row[' finance_indi_ipominamnt '],
                        'finance_next_openday': row[' finance_next_openday'],
                        'finance_ipoapp_flag': row[' finance_ipoapp_flag '],
                        'cpbm': row[' finance_no '],
                        'mjjsrq': row[' finance_ipo_enddate '],
                        'fxdj': row[' finance_risklevel '],
                        'yzms': typeName,
                        'logId': log_id
                    }
                    # row = {k: str(v).replace('\'', '') for k, v in row.items()}
                    insertLogToDB(cur, row, TARGET_TABLE)
        cur.connection.commit()
        updateLogToDB(cur, log_id, {'status': '完成《固定期限》类别的数据', 'updateTime': getLocalDate()})
        cur.connection.commit()


# 移动端
def process_mobile(conn, cursor, session: Session, log_id: int, **kwargs):
    """
    处理爬虫细节的回调函数
    :param conn:自动传参
    :param cursor: 自动传参
    :param session: 自动传参
    :param log_id: 自动传参
    :param kwargs: 额外需要的参数 手动传入
    :return:
    """
    for type in TYPES_MOBILE:
        request_url = type['url']
        request_method = type['method']
        request_params = type['requestParams']
        resp = session.request(method=request_method, url=request_url, data=json.dumps(request_params),
                               headers=HEADERS_MOBILE, verify=False).text
        # time.sleep(3)
        # 获取总页数
        rep = json.loads(resp)['RspBody']
        Num = rep['Finance_Num']
        # total_page = get_total_page(resp)
        # 循环处理每页数据
        # loop_pages(total_, session, cursor, log_id, request_url, request_method, request_params)

        request_params['ReqBody']['QueryNumber'] = str(Num)
        print(Num)
        # process_one_page(session, cursor, log_id, request_url, request_method, request_params)
        resp = session.request(method=request_method, url=request_url, data=json.dumps(request_params),
                               headers=HEADERS_MOBILE).text
        # time.sleep(3)
        # rows = parse_table(resp)
        res = json.loads(resp)
        if res:
            result = res['RspBody']
            productList = result['List']
            rows = []
            for product in productList:
                productNo = product['Finance_No']
                PARAMS_MOBILE_DETAIL['ReqBody']['FinanceNo'] = productNo
                resp = session.request(method=MOBILE_DETAIL_METHOD, url=MOBILE_DETAIL_URL, data=json.dumps(PARAMS_MOBILE_DETAIL),headers=HEADERS_MOBILE_DETAIL).text
                if resp:
                    res = json.loads(resp)
                    result = res['RspBody']
                    row = {
                        'cpbm': productNo,
                        'dzje': result['FinanceSupplementAmnt'],
                        'fsqsrq': result['FinanceStartDate'],
                        'khsygz': result['FinanceEarningsRule'],
                        'FinanceAbbrName': result['FinanceAbbrName'],
                        'EVoucherCode': result['EVoucherCode'],
                        'RaiseType': result['RaiseType'],
                        'FinanceOwner': result['FinanceOwner'],
                        'RegionOpenFlag': result['RegionOpenFlag'],
                        'FinanceShowBenchmark': result['FinanceShowBenchmark'],
                        'FinanceProductLabelUrl': result['FinanceProductLabelUrl'],
                        'FinanceMinHoldings': result['FinanceMinHoldings'],
                        'PerformanceRewards': result['PerformanceRewards'],
                        'FinanceReturnRateNameDesc': result['FinanceReturnRateNameDesc'],
                        'FinanceCurrency': result['FinanceCurrency'],
                        'cpclr': result['FinanceFoundedDate'],
                        'cpmc': result['FinanceName'],
                        'FinanceFee': result['FinanceFee'],
                        'FinanceShowMsgMark': result['FinanceShowMsgMark'],
                        'FinancePurchaseStartTime': result['FinancePurchaseStartTime'],
                        'FinanceEarningsFlag': result['FinanceEarningsFlag'],
                        'FinanceExpireDate': result['FinanceExpireDate'],
                        'FinanceKind': result['FinanceKind'],
                        'CurrentDate': result['CurrentDate'],
                        'qgje': result['FinanceMinAmount'],
                        'HasHoldings': result['HasHoldings'],
                        'RegistrantNo': result['RegistrantNo'],
                        'FinanceBenchmarkDesc': result['FinanceBenchmarkDesc'],
                        'fxjg': result['FinanceIssuer'],
                        'ChartTips': result['ChartTips'],
                        'EVoucherCodeMaxAmnt': result['EVoucherCodeMaxAmnt'],
                        'EndEntryRule': result['EndEntryRule'],
                        'InvestmentOrientation': result['InvestmentOrientation'],
                        'FinanceFastRedemptionRule': result['FinanceFastRedemptionRule'],
                        'IsFavorite': result['IsFavorite'],
                        'EVoucherCodeMinAmnt': result['EVoucherCodeMinAmnt'],
                        'fsjzrq': result['FinanceEndDate'],
                        'FinanceRedemptionStartDate': result['FinanceRedemptionStartDate'],
                        'FinanceReturnRateName': result['FinanceReturnRateName'],
                        'FinanceBrand': result['FinanceBrand'],
                        'FinanceTransStage': result['FinanceTransStage'],
                        'FinanceBenchmark': result['FinanceBenchmark'],
                        'EndRule': result['EndRule'],
                        'syqsrq': result['FinanceEarningsStartDate'],
                        'RiskAlert': result['RiskAlert'],
                        'ProductSegmentFlag': result['ProductSegmentFlag'],
                        'FinanceReturnRateLevel': result['FinanceReturnRateLevel'],
                        # 'FinanceShareLevel': result['FinanceShareLevel'],
                        'FinanceEarningsType': result['FinanceEarningsType'],
                        'FinanceShowYieldEstimation': result['FinanceShowYieldEstimation'],
                        'FinanceRedemptionFee': result['FinanceRedemptionFee'],
                        'DigitalHuman': result['DigitalHuman'],
                        'tzqx': result['FinanceValidityDesc'],
                        'FinancePurchaserNum': result['FinancePurchaserNum'],
                        'cpbm': result['FinanceNo'],
                        'FinanceReturnRate': result['FinanceReturnRate'],
                        'fxdj': result['FinanceRiskLevel'],
                        'AssetMgmtIntro': result['AssetMgmtIntro'],
                        'FinanceType': result['FinanceType'],
                        'PerformanceRewardsDesc': result['PerformanceRewardsDesc'],
                        'shgz': result['FinanceRedemptionStartTime'],
                        'FinanceFastRedemptionMaxAmnt': result['FinanceFastRedemptionMaxAmnt'],
                        'FinanceValidity': result['FinanceValidity'],
                        'dzgz': result['FinanceRedemptionRule'],
                        'qxgz': result['FinanceTransStartRule'],
                        'FinanceMinRedemption': result['FinanceMinRedemption'],
                        'DefaultTab': result['DefaultTab'],
                        # 'MovementsType': result['MovementsType'],
                        'FinanceExpectedYield': result['FinanceExpectedYield']
                    }
                    rows.append(row)
        if not rows == None:
            for row in rows:
                row['logId'] = log_id
                row['createTime'] = getLocalDate()
                update_else_insert_to_db(cursor, TARGET_TABLE, row, {'logid': log_id, 'cpbm': row['cpbm']})


# if __name__ == '__main__':
#     conn = None
#     cursor = None
#     generated_log_id = None
#     session = None
#     try:
#         conn = get_conn_oracle()
#         cursor = conn.cursor()
#         # 记录开始日志
#         mark_start_log(NAME, getLocalDate(), cursor)
#         # 获取日志id
#         generated_log_id = get_generated_log_id(NAME, cursor)
#         # 处理爬起数据的逻辑
#         session = requests.session()
#         process_spider(session, cursor, generated_log_id)
#         process_mobile(conn, cursor, session, generated_log_id, **kwargs)
#         # 统计插入的数据条数
#         count = get_write_count(TARGET_TABLE, generated_log_id, cursor)
#         # 记录成功日志
#         mark_success_log(count, getLocalDate(), generated_log_id, cursor)
#     except Exception as e:
#         print(e)
#         # 回滚事务
#         if cursor:
#             cursor.connection.rollback()
#         # 记录失败日志
#         if generated_log_id:
#             mark_failure_log(e, getLocalDate(), generated_log_id, cursor)
#     finally:
#         close([cursor, conn, session])


class SpiderFlowImpl(SpiderFlow):
    def callback(self, conn, cursor, session: Session, log_id: int, **kwargs):
        proxy = None
        # f = open('crawl.txt', 'w')
        try:
            # process_spider(conn,cursor, session, log_id,**kwargs)
            process_mobile(conn, cursor, session, log_id, **kwargs)
            # process(cursor, session, 874, proxy)
        except Exception as e:
            raise e
        finally:
            if proxy:
                proxy.close()
            # if f:
            #     f.close()


if __name__ == '__main__':
    # process_flow(log_name=LOG_NAME,target_table=TAGET_TABLE,callback=process_spider_detail)
    process_flow(NAME, TARGET_TABLE, SpiderFlowImpl())
