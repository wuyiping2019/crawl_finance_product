# import json
# import math
# import time
#
# from browsermobproxy import Server
# from cx_Oracle import _Error
# from requests import Session
# from selenium import webdriver
# from selenium.webdriver.common.by import By
#
# from utils.db_utils import check_table_exists, create_table, process_dict, check_field_exists, add_field
# from utils.global_config import DB_ENV, DBType, get_field_type
# from utils.mark_log import insertLogToDB
# from utils.selenium_utils import get_driver
# from utils.spider_flow import SpiderFlow, process_flow
#
# REQUEST_URL_PC = 'http://www.cmbc.com.cn/gw/po_web/QryProdListOnMarket.do'
# RQUEST_URL_MOBILE = 'https://m1.cmbc.com.cn/gw/m_app/FinFundProductList.do'
#
# MARK_STR = 'zxyh'
# TARGET_TABLE_PC = f'ip_bank_{MARK_STR}_pc'
# TARGET_TABLE_MOBILE = f'ip_bank_{MARK_STR}_mobile'
# PROCESSED_TABLE = f'ip_bank_{MARK_STR}'
# LOG_NAME = '中信银行'
# SEQUENCE_NAME = f'seq_{MARK_STR}'
# TRIGGER_NAME = f'trigger_{MARK_STR}'
#
# CRAWL_REQUEST_DETAIL_TABLE = f'crawl_{MARK_STR}'
#
# HEADERS_PC = {
#     'Accept': 'application/json, text/javascript, */*; q=0.01',
#     'Accept-Encoding': 'gzip, deflate',
#     'Accept-Language': 'zh-CN,zh;q=0.9',
#     'Cache-Control': 'no-cache',
#     'Content-Length': '261',
#     'Content-Type': 'application/json;charset=UTF-8',
#     'Host': 'www.cmbc.com.cn',
#     'Origin': 'http://www.cmbc.com.cn',
#     'Pragma': 'no-cache',
#     'Proxy-Connection': 'keep-alive',
#     'Referer': 'http://www.cmbc.com.cn/xmhpd/grkh/rmcp/rmlc/index.htm',
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
#     'X-Requested-With': 'XMLHttpRequest'
# }
#
# DATA_PC = {
#     "currTypeList": [],
#     "keyWord": "",
#     "currentIndex": 0,
#     "fundModeList": [],
#     "orderFlag": "1",
#     "pageNo": 1,
#     "pageSize": 10,
#     "pfirstAmtList": [],
#     "prdChara": "4",
#     "prdTypeNameList": [],
#     "$FF_HEADER$": {
#         "appId": "",
#         "appVersion": "",
#         "device": {
#             "osType": "BROWSER",
#             "osVersion": "",
#             "uuid": ""
#         }
#     }
# }
# METHOD_PC = 'post'
#
# HEADERS_MOBILE = {
#     'Accept': 'application/json, text/plain, */*',
#     'Accept-Encoding': 'gzip, deflate, br',
#     'Accept-Language': 'zh-CN,zh;q=0.9',
#     'Cache-Control': 'no-cache',
#     'Connection': 'keep-alive',
#     'Content-Length': '376',
#     'Content-Type': 'application/json;charset=UTF-8',
#     'Cookie': 'bigipW_http_m1_cookie=!JMw3W/TLrQr3JNCCPONelVKPr9b9hNzR7qLmsTYkkgy4ROINtBrrNB6sRiQQV/+mlPyeWlL1m6iprVE=; org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE=zh_CN; BIGipServerUEB_tongyidianziqudao_app_41002_pool=!AGsmAgE2oNy6Ow80lXP1ySZhZOpxgn+ljwTg3TwveXJrkwDMUJzy6kaGgUFhgfHvmqfczS9Gt87N0Jo=; BIGipServershoujiyinhang_geren_app_8000_pool=!9rJRT9zEmDsOmbw0lXP1ySZhZOpxghX+uVxDZvpkzEwr7Va/HYQrDPcepa73CuxxMIUyfR7VJD0aCgQ=; RSESSIONID=F09803945F90AD3B138F4D6ADA6D6FB3; lastAccessTime=1661924631194; JSESSIONID=5m7ybWWy8rtpx9paG5Ezk6O9SfhtGrCfnzdktw8RxYxBlCGI7OV7!611416433',
#     'Host': 'm1.cmbc.com.cn',
#     'Origin': 'https://m1.cmbc.com.cn',
#     'Pragma': 'no-cache',
#     'Referer': 'https://m1.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/fund-commission/prd-list',
#     'sec-ch-ua': '"Google Chrome";v="95", "Chromium";v="95", ";Not A Brand";v="99"',
#     'sec-ch-ua-mobile': '?1',
#     'sec-ch-ua-platform': '"Android"',
#     'Sec-Fetch-Dest': 'empty',
#     'Sec-Fetch-Mode': 'cors',
#     'Sec-Fetch-Site': 'same-origin',
#     'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36'
# }
#
# DTAT_MOBILE = {
#     "request": {
#         "header": {
#             "appId": "",
#             "appVersion": "web",
#             "appExt": "999",
#             "device": {
#                 "appExt": "999",
#                 "osType": "03",
#                 "osVersion": "",
#                 "uuid": ""
#             }
#         },
#         "body": {
#             "pageSize": 10,
#             "currentIndex": 0,  # 动态修改
#             "startId": 1,  # 动态修改
#             "prdTypeNameList": [],
#             "sellingListTypeIndex": "4",
#             "sortFileldName": "LIQU_MODE_NAME",
#             "sortFileldType": "DESC",
#             "prdChara": "",
#             "liveTime": "",
#             "pfirstAmt": "",
#             "currType": "",
#             "keyWord": "",
#             "isKJTSS": "0"
#         }
#     }
# }
#
# METHOD_MOBILE = 'post'
#
#
# def process_pc(cursor, session: Session, log_id: int):
#     # 处理PC端原始请求数据
#     # 1.ORIGIN_TABLE
#     flag = check_table_exists(TARGET_TABLE_PC, cursor)
#     resp = session.request(method=METHOD_PC, url=REQUEST_URL_PC, json=DATA_PC, headers=HEADERS_PC).text
#     total_count = int(json.loads(resp)['totalSize'])
#     page_count = math.ceil(total_count / DATA_PC['pageSize'])
#     for i in range(1, page_count + 1):
#         time.sleep(3)
#         DATA_PC['pageNo'] = i
#         resp = session.request(method=METHOD_PC, url=REQUEST_URL_PC, json=DATA_PC, headers=HEADERS_PC).text
#         loads = json.loads(resp)
#         prd_list = loads['prdList']
#         for prd in prd_list:
#             prd = process_dict(prd)
#             # 不存在表
#             if not flag:
#                 create_table(prd,
#                              'varchar2(1000)',
#                              'number(11)',
#                              TARGET_TABLE_PC,
#                              cursor,
#                              SEQUENCE_NAME,
#                              TRIGGER_NAME)
#                 flag = True
#             if flag:
#                 prd['logId'] = log_id
#                 try:
#                     insertLogToDB(cursor, prd, TARGET_TABLE_PC)
#                 except Exception as e:
#                     args_: _Error = e.args[0]
#                     if args_.code == 904:
#                         # 表示存在字段不存在的问题
#                         for field in prd.keys():
#                             if not check_field_exists(str(field), TARGET_TABLE_PC, cursor):
#                                 add_field(str(field), get_field_type()[0])
#                         # 重新插入这条数据
#                         insertLogToDB(cursor, prd, TARGET_TABLE_PC)
#         cursor.connection.commit()
#
#
# def process_mobile(cursor, session: Session, log_id: int):
#     # 处理移动端请求的原始数据
#     # 1.ORIGIN_TABLE
#     flag = check_table_exists(TARGET_TABLE_MOBILE, cursor)
#     resp = session.request(method=METHOD_MOBILE, url=RQUEST_URL_MOBILE, json=DTAT_MOBILE, headers=HEADERS_MOBILE).text
#     loads = json.loads(resp)
#     total_size = loads['response']['totalSize']  # 64
#     page_count = math.ceil(total_size / (DTAT_MOBILE['request']['body']['pageSize']))
#     for page in range(1, page_count + 1):
#         time.sleep(3)
#         DTAT_MOBILE['request']['body']['currentIndex'] = 10 * (page - 1)
#         DTAT_MOBILE['request']['body']['startId'] = 10 * (page - 1) + 1
#         # flag标识是否存在相应的存储数据的表
#         flag = check_table_exists(TARGET_TABLE_MOBILE, cursor)
#         for prd in loads['response']['list']:
#             if not flag:
#                 create_table(prd,
#                              get_field_type()[0],
#                              get_field_type()[1],
#                              TARGET_TABLE_MOBILE,
#                              cursor,
#                              SEQUENCE_NAME + f'_{len(TARGET_TABLE_MOBILE)}',
#                              TRIGGER_NAME + f'_{len(TARGET_TABLE_MOBILE)}')
#                 # 创建表之后将flag设置为true
#                 flag = True
#             # 当表存在时,向表中插入数据
#             if flag:
#                 try:
#                     prd['pageNo'] = page
#                     prd['logId'] = log_id
#                     insertLogToDB(cursor, prd, TARGET_TABLE_MOBILE)
#                 except Exception as e:
#                     args_: _Error = e.args[0]
#                     if args_.code == 904:
#                         # 表示存在字段不存在的问题
#                         for field in prd.keys():
#                             if not check_field_exists(str(field), TARGET_TABLE_PC, cursor):
#                                 add_field(str(field), get_field_type()[0], TARGET_TABLE_MOBILE, cursor)
#                         # 重新插入这条数据
#                         insertLogToDB(cursor, prd, TARGET_TABLE_MOBILE)
#         cursor.connection.commit()
#
#
# def load_more_product(driver):
#     driver.find_element(By.CLASS_NAME, 'am-list-footer').click()
#
#
# def locate_footer(driver):
#     while True:
#         try:
#             footer_element = driver.find_element(By.CLASS_NAME, 'am-list-footer')
#             break
#         except Exception as e:
#             print(e)
#             time.sleep(3)
#     return footer_element
#
#
# def process(cursor, session: Session, log_id: int, proxy):
#     """
#     将数据写入PROCESSED_TABLE中
#     :param f:
#     :param proxy:
#     :param cursor:
#     :param session:
#     :param log_id:
#     :return:
#     """
#     options = webdriver.ChromeOptions()
#     mobile_emulation = {"deviceName": "iPhone X"}
#     time.sleep(3)
#     options.add_experimental_option("mobileEmulation", mobile_emulation)
#     options.add_argument('--ignore-certificate-errors')
#     options.add_argument('--proxy-server={}'.format(proxy.proxy))
#     options.add_experimental_option("excludeSwitches", ["ignore-certificate-errors"])
#     driver = get_driver(options=options)
#     proxy.new_har(ref="product_list_page", options={'captureContent': True, 'captureBinaryContent': True})
#     driver.get('https://m1.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/fund-commission/prd-list')
#     time.sleep(3)
#     current_index = 0
#     # 从此处开始
#     proxy.new_har(ref="product_detail_page",
#                   options={
#                       'captureContent': True,
#                       'captureBinaryContent': True
#                   }
#                   )
#
#     product_count = len(driver.find_elements(By.CLASS_NAME, 'name'))
#     while current_index + 1 <= product_count:
#         rs: list = proxy.har['log']['entries']
#         while len(rs) != 0:
#             request_info = rs.pop(0)
#             save_dict = {
#                 'pageref': request_info['pageref'],
#                 'url': request_info['request']['url'],
#                 'startedDataTime': request_info['startedDateTime'],
#                 'response': json.dumps(request_info['response'])
#             }
#
#             flag = check_table_exists(CRAWL_REQUEST_DETAIL_TABLE, cursor)
#             if not flag:
#                 create_table(save_dict,
#                              'clob',
#                              'number(11)',
#                              CRAWL_REQUEST_DETAIL_TABLE,
#                              cursor,
#                              SEQUENCE_NAME,
#                              TRIGGER_NAME)
#             flag = True
#             if flag:
#                 sql = f"""
#                         insert into {CRAWL_REQUEST_DETAIL_TABLE} (logid,pageref,startedDataTime,response,url)
#                         values (:logid,:pageref,:startedDataTime,:response,:url)
#                         """
#                 cursor.execute(sql, [log_id, request_info['pageref'], request_info['startedDateTime'],
#                                      json.dumps(request_info['response']),
#                                      request_info['request']['url']
#                                      ]
#                                )
#                 cursor.connection.commit()
#             if current_index == 11:
#                 return
#         # 获取一个产品数据的详情
#         current_element = driver.find_elements(By.CLASS_NAME, 'name')[current_index]
#         current_element.click()
#         time.sleep(3)
#         # 更新current_index
#         current_index += 1
#         # 返回到上一页
#         proxy.new_har(ref="product_list_page", options={'captureContent': True, 'captureBinaryContent': True})
#         driver.back()
#         time.sleep(3)
#         # 返回上一页之后 重置product_count
#         product_count = len(driver.find_elements(By.CLASS_NAME, 'name'))
#         # 继续获取下一个产品数据的详情
#         # 但是可能存在  current_index + 1 > product_count的情况 导致跳出循环
#         # 而跳出循环时 只是由于更多的产品数据没有加载
#         if current_index + 1 <= product_count:
#             # 该情况可以继续循环
#             continue
#         else:
#             # 循环加载更多的数据 直至满足外面的循环条件或者已经加载到底部了
#             while True:
#                 # 加载更多数据
#                 load_more_product(driver)
#                 time.sleep(3)
#                 # 更新product_count
#                 product_count = len(driver.find_elements(By.CLASS_NAME, 'name'))
#                 footer_element = locate_footer(driver)
#                 if current_index + 1 <= product_count or footer_element.text == '加载完毕':
#                     # 当满足外层循环条件或者已经加载到底部了
#                     # 跳出当前加载数据的循环 让外部循环接管逻辑
#                     break
#
# class SpiderFlowImpl(SpiderFlow):
#     def callback(self, conn, cursor, session: Session, log_id: int, **kwargs):
#         proxy = None
#         f = open('crawl.txt', 'w')
#         try:
#             proxy_server = Server(r'E:\PycharmProjects\kb_graph_sync\browsermob-proxy-2.1.4\bin\browsermob-proxy.bat')
#             proxy_server.start()
#             proxy = proxy_server.create_proxy({'trustAllServers': True})
#             # process_pc(cursor, session, log_id)
#             # process_mobile(cursor, session, log_id)
#             process(cursor, session, 874, proxy)
#         except Exception as e:
#             raise e
#         finally:
#             if proxy:
#                 proxy.close()
#             if f:
#                 f.close()
#
#
# if __name__ == '__main__':
#     process_flow(LOG_NAME, PROCESSED_TABLE, SpiderFlowImpl())
