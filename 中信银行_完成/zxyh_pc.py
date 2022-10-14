import json
import math
import time
from typing import List

from requests import Response

from crawl_utils.crawl_request import ConfigurableCrawlRequest, RowFilter
from crawl_utils.db_utils import getLocalDate
from 中信银行_完成.zxyh_config import PC_REQUEST_URL, PC_REQUEST_METHOD, PC_REQUEST_HEADERS, PC_REQUEST_JSON, \
    PC_FIELD_NAME_2_NEW_FIELD_NAME, MASK


class ZxyhPCCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super(ZxyhPCCrawlRequest, self).__init__(name='中信银行PC')
        self.page_no = None
        self.total_page = None

    def _pre_crawl(self):
        self.field_name_2_new_field_name = PC_FIELD_NAME_2_NEW_FIELD_NAME
        self.mask = MASK
        self.check_props = ['logId', 'cpbm', 'bank']
        self.request['url'] = PC_REQUEST_URL
        self.request['method'] = PC_REQUEST_METHOD
        self.request['headers'] = PC_REQUEST_HEADERS

        row_filter = RowFilter()
        row_filter.filter = self.get_detail_info
        self.add_filter(row_filter)

    def _config_params(self):
        if self.page_no is None:
            self.page_no = 1
        else:
            self.page_no += 1
        self.request['json'] = PC_REQUEST_JSON(self.page_no)

    def _parse_response(self, response: Response) -> List[dict]:
        loads = json.loads(response.text)
        if self.total_page is None:
            total_size = loads['response']['totalSize']
            self.total_page = math.ceil(total_size / len(loads['response']['list']))
        return loads['response']['list']

    def _config_end_flag(self):
        if self.page_no is not None and self.total_page is not None and self.page_no == self.total_page:
            self.end_flag = True

    def _row_processor(self, row: dict) -> dict:
        return row

    def _row_post_processor(self, row: dict):
        row['logId'] = self.log_id
        row['createTime'] = getLocalDate()
        row['mark'] = 'PC'
        row['bank'] = '中信银行'
        return row

    def get_detail_info(self, row: dict):
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
                            "prdCode": row['cpbm']
                        }
                }
        }
        response = self.session.post(url=url, json=data, headers=PC_REQUEST_HEADERS)
        time.sleep(1)
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
                ).encode().decode("unicode_escape")
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
        detail_row = {
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
            ).encode().decode('unicode_escape'),
            'shsj': str(fund_detail['RED_DAYS']) + '天',
            'mjbz': str(fund_detail['CURR_TYPE_NAME'])
        }
        detail_row.update(attrs)
        row.update(detail_row)
        return row


if __name__ == '__main__':
    ZxyhPCCrawlRequest().init_props(log_id=1).do_crawl()
