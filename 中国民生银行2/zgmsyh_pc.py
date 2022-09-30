import json
import math
from typing import List

from requests import Response

from utils.crawl_request import AbstractCrawlRequest

# http://www.cmbc.com.cn/xmhpd/grkh/rmcp/rmlc/index.htm
from utils.mappings import FIELD_MAPPINGS


class ZgmsyhCrawlRequest(AbstractCrawlRequest):

    def _set_request_json(self):
        # 1.获取page_no
        page_no = getattr(self, 'page_no')
        # 2.根据page_no设置self.request['json']参数
        self.request['json'] = getattr(self, 'set_request')['json'](page_no)

    def _prep_request(self):
        """
        初始化请求参数
        :return:
        """
        # 1.设置page_no
        setattr(self, 'page_no', 1)
        # 2.初始化self.request中的json参数
        self._set_request_json()

    def _parse_response(self, response: Response) -> List[dict]:
        # 1.获取Response的body内容
        resp_str = response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text
        # 2.将body文本内容转为字典
        loads = json.loads(resp_str)
        # 3.判断是否已设置total_page
        total_page = getattr(self, 'page_no', None)
        if not total_page:
            # 设置total_page
            total_page = math.ceil(loads['totalSize'] / len(loads['prdList']))
            setattr(self, 'total_page', total_page)
        # 4.获取产品列表
        rows = loads['prdList']
        # 5.返回产品列表
        return rows

    def _row_processor(self, row: dict) -> dict:
        # 1.处理业绩比较基准

        return row

    def _row_post_processor(self, row: dict):
        # 1.在原字典中添加logId和createTime两个字段
        row = super()._row_post_processor()
        # 2.返回结果
        return row

    def _if_end(self, response: Response) -> bool:
        pass

    def _next_request(self):
        # 1.更新page_no
        page_no = getattr(self, 'page_no')
        setattr(self, 'page_no', page_no + 1)
        # 2.设置self.request['json']请求参数
        self._set_request_json()


zgmsyh_crawl_pc = ZgmsyhCrawlRequest(
    request={
        'url': 'http://www.cmbc.com.cn/gw/po_web/QryProdListOnMarket.do',
        'headers': {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Content-Length": "261",
            "Content-Type": "application/json;charset=UTF-8",
            "Host": "www.cmbc.com.cn",
            "Origin": "http//www.cmbc.com.cn",
            "Pragma": "no-cache",
            "Proxy-Connection": "keep-alive",
            "Referer": "http//www.cmbc.com.cn/xmhpd/grkh/rmcp/rmlc/index.htm",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
        }
    },
    set_request={
        'json': lambda page_no: {
            "currTypeList": [],
            "keyWord": "",
            "currentIndex": 0,
            "fundModeList": [],
            "orderFlag": "1",
            "pageNo": page_no,
            "pageSize": 10,
            "pfirstAmtList": [],
            "prdChara": "4",
            "prdTypeNameList": [],
            "$FF_HEADER$": {
                "appId": "",
                "appVersion": "",
                "device": {
                    "osType": "BROWSER",
                    "osVersion": "",
                    "uuid": ""
                }
            }
        }
    },
    field_name_2_new_field_name={
        'prdName': FIELD_MAPPINGS['产品名称'],
        'prdShortName': FIELD_MAPPINGS['产品简称'],
        'prdCode': FIELD_MAPPINGS['产品编码'],
        'currTypeName': FIELD_MAPPINGS['币种'],
        'pfirstAmt': FIELD_MAPPINGS['起购金额'],
        'riskLevelName': FIELD_MAPPINGS['风险等级'],
        'ipoStartDate': FIELD_MAPPINGS['募集起始日期'],
        'ipoEndDate': FIELD_MAPPINGS['募集结束日期'],
        'totAmt': FIELD_MAPPINGS['产品额度'],
        'usableAmt': FIELD_MAPPINGS['剩余额度'],
        'startDate': FIELD_MAPPINGS['产品起始日期'],
        'endDate': FIELD_MAPPINGS['产品结束日期'],
        'nextOpenDate': FIELD_MAPPINGS['下一个开放日'],
        'prdTypeName': FIELD_MAPPINGS['运作模式'],
        'NAV': FIELD_MAPPINGS['净值'],
        'navDate': FIELD_MAPPINGS['净值日期'],
        'prdManagerName': FIELD_MAPPINGS['管理人']
    }
)
