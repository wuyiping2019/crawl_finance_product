from typing import Dict, List

from requests import Session

from utils.common_utils import delete_empty_value
from utils.mappings import FIELD_MAPPINGS


class CrawlRequestException(Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg


class CrawlRequest:
    def __init__(self,
                 method,
                 url,
                 params=None,
                 data=None,
                 json=None,
                 headers=None,
                 missing_code_sample=3,
                 **kwargs):
        self.request = self.do_init(method, url, params, data, json, headers, **kwargs)
        self.mark_code_mapping = {}
        self.missing_code_sample = missing_code_sample

    def do_crawl(self,
                 session: Session,
                 to_rows: callable,
                 func: Dict[str, callable],
                 mapping_dicts: Dict[str, Dict[str, str]],
                 transformed_keys: Dict[str, str]
                 ):
        response = session.request(**self.request)
        rows: List[dict] = to_rows(response)
        processed_rows = []
        for row in rows:
            processed_row = self.do_row_mapping(row, func, mapping_dicts, transformed_keys)
            if processed_row:
                processed_rows.append(processed_row)
        return processed_rows

    @staticmethod
    def do_init(method, url, params=None, data=None, json=None, headers=None, **kwargs):
        request_params = {
            'method': method,
            'url': url,
            'params': params,
            'data': data,
            'json': json,
            'headers': headers
        }
        for k, v in kwargs.items():
            if k:
                request_params[k] = v
        # 删除空key/value
        delete_empty_value(request_params)
        return request_params

    def do_row_mapping(self,
                       row: dict,
                       func: Dict[str, callable],
                       mapping_dicts: Dict[str, Dict[str, str]],
                       transformed_keys: Dict[str, str],
                       ) -> dict:
        processed_row = {}
        for k, v in row.items():
            temp_dict = self.__field_mapping(row, k, func.get(k, None),
                                             mapping_dicts.get(k, None),
                                             transformed_keys.get(k, None))
            processed_row.update(temp_dict)
        delete_empty_value(processed_row)
        return processed_row

    def __field_mapping(self,
                        row: dict,
                        row_field: str,
                        func: callable = None,
                        mapping_dict: Dict[str, str] = None,
                        transformed_key: str = None) -> dict:
        """
        对一个key/value进行转换
        返回单个键值对的字典
        :param row: 爬取的原始字典数据
        :param row_field: 需要转换的字段
        :param func: 转换字典使用的函数
        :param mapping_dict: 转换字典需要使用的映射字典
        :param transformed_key: 转换之后使用的FIELD_MAPPINGS中的key值
        :return: 返回单个键值对的dict
        """
        if row_field not in row.keys():
            raise CrawlRequestException(None, f'__field_mapping:(传入的row:{row}中不存在row_field:{row_field}的key值)')
        if transformed_key not in FIELD_MAPPINGS.keys():
            raise CrawlRequestException(None,
                                        f'__field_mapping:(transformed_key:{transformed_key}必须存在于FIELD_MAPPINGS:{FIELD_MAPPINGS.keys()}中)')

        value = row[row_field]
        # 对value进行转换
        if callable:
            return {
                FIELD_MAPPINGS[transformed_key]: func(value)
            }

        if mapping_dict and transformed_key:
            if transformed_key not in FIELD_MAPPINGS.keys():
                raise CrawlRequestException(None,
                                            f'__field_mapping:(传入的transformed_key:{transformed_key}中不存在FIELD_MAPPINGS:{FIELD_MAPPINGS}的key值)')
            # 需要记录日志 表示没有配置映射
            if value not in mapping_dict:
                if row_field not in self.mark_code_mapping:
                    self.mark_code_mapping[row_field] = {}
                if value not in self.mark_code_mapping[row_field]:
                    self.mark_code_mapping[row_field][value] = []
                if len(self.mark_code_mapping[row_field][value]) <= self.missing_code_sample:
                    self.mark_code_mapping[row_field][value].append(row)
            return {
                FIELD_MAPPINGS[transformed_key]: mapping_dict.get(value, None)
            }
        elif transformed_key:
            return {
                FIELD_MAPPINGS[transformed_key]: value
            }
        else:
            raise CrawlRequestException(None,
                                        f'__field_mapping:(传入的mapping_dict:(字段映射func参数和transformed_key参数)')


PAYH_PC_REQUEST = {
    '银行理财':
        {
            'method': 'post',
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'data': lambda page_num: {
                'tableIndex': 'table01',
                'dataType': '01',
                'tplId': 'tpl01',
                'pageNum': f'{page_num}',
                'pageSize': '10',
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {
                "host": "ebank.pingan.com.cn",
                "content-length": "99",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
                "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                "accept": "*/*",
                "origin": "https//ebank.pingan.com.cn",
                "sec-fetch-site": "same-origin",
                "sec-fetch-mode": "cors",
                "sec-fetch-dest": "empty",
                "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "zh-CN,zh;q=0.9",
                "cookie": "WEBTRENDS_ID=2143a2a868e936ec8091661310781821; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHmbEvCoJgJ6uovdqduWl0zvJ-a6UMZB; WT-FPC=id=2143a2a868e936ec8091661310781821lv=1661757231769ss=1661757218522fs=1661310781821pn=7vn=3; BSFIT4_EXPIRATION=1661838937956; BSFIT4_DEVICEID=g8FY5G6z6QRgNFkhFvHyTgPLPOb8g_bEKOL0ckxrplveiPqIA6pG71I2aNM2OGY1c6sdB54fi7hxSKxLqmrbPkhxfdqEdMcDeQsVfjAahc_kzC7yJASKkAGIt5viwrKM92jhNOx1yb-deWCc_LfNefay98HrSTRo"
            },
            'row_mapping': lambda row: {
                FIELD_MAPPINGS['起购金额']: row.get('minAmount', '') + '元',
                FIELD_MAPPINGS['风险等级']: ROW_MAPPING_COFNIG['银行理财']['riskLevel'][row.get('riskLevel', '')],
                FIELD_MAPPINGS['产品名称']: row.get('prdName', ''),
                FIELD_MAPPINGS['管理人']: row.get('manageBankName', ''),
                FIELD_MAPPINGS['产品状态']: ROW_MAPPING_COFNIG['银行理财']['saleStatus'][row.get('saleStatus', '')],
                FIELD_MAPPINGS['产品编码']: row.get('prdCode', ''),
                FIELD_MAPPINGS['收益类型']: ROW_MAPPING_COFNIG['银行理财']['rateType'][
                    row.get('rateType', '')],
                FIELD_MAPPINGS['TA编码']: row.get('taCode', ''),
                FIELD_MAPPINGS['销售渠道']: row.get('tranBankSet', ''),
                FIELD_MAPPINGS['登记编码']: row.get('productNo', '')
            },

        },
    '银行公司理财':
        {
            'method': 'post',
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'data': lambda page_num: {
                'tableIndex': 'table05',
                'dataType': '05',
                'tplId': 'tpl03',
                'pageNum': f'{page_num}',
                'pageSize': '10',
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {
                "host": "ebank.pingan.com.cn",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
                "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                "accept": "*/*",
                "origin": "https//ebank.pingan.com.cn",
                "sec-fetch-site": "same-origin",
                "sec-fetch-mode": "cors",
                "sec-fetch-dest": "empty",
                "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "zh-CN,zh;q=0.9",
                "cookie": "WEBTRENDS_ID=2143a2a868e936ec8091661310781821; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHmbEvCoJgJ6uovdqduWl0zvJ-a6UMZB; WT-FPC=id=2143a2a868e936ec8091661310781821lv=1661757231769ss=1661757218522fs=1661310781821pn=7vn=3; BSFIT4_EXPIRATION=1661838937956; BSFIT4_DEVICEID=g8FY5G6z6QRgNFkhFvHyTgPLPOb8g_bEKOL0ckxrplveiPqIA6pG71I2aNM2OGY1c6sdB54fi7hxSKxLqmrbPkhxfdqEdMcDeQsVfjAahc_kzC7yJASKkAGIt5viwrKM92jhNOx1yb-deWCc_LfNefay98HrSTRo"
            },
            'row_mapping': lambda row: {
                FIELD_MAPPINGS['起购金额']: row.get('minAmount', '') + '元',
                FIELD_MAPPINGS['风险等级']: ROW_MAPPING_COFNIG['银行公司理财']['riskLevel'][row.get('riskLevel', '')],
                FIELD_MAPPINGS['产品名称']: row.get('prdName', ''),
                FIELD_MAPPINGS['产品状态']: ROW_MAPPING_COFNIG['银行公司理财']['saleStatus'][
                    row.get('saleStatus', '')],
                FIELD_MAPPINGS['产品编码']: row.get('prdCode', ''),
                FIELD_MAPPINGS['登记编码']: row.get('productNo', ''),
                FIELD_MAPPINGS['收益类型']: ROW_MAPPING_COFNIG['银行公司理财']['rateType'][
                    row.get('rateType', '')],
                FIELD_MAPPINGS['管理人']: '平安银行',
                FIELD_MAPPINGS['销售渠道']: '全行',
            }
        },
    '对公结构性存款':
        {
            'method': 'post',
            'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
            'data': lambda page_num: {
                'tableIndex': 'table10',
                'dataType': '10',
                'tplId': 'tpl10',
                'pageNum': f'{page_num}',
                'pageSize': '10',
                'channelCode': 'C0002',
                'access_source': 'PC',
            },
            'headers': {
                "host": "ebank.pingan.com.cn",
                "content-length": "99",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
                "content-type": "application/x-www-form-urlencoded;charset=utf-8",
                "accept": "*/*",
                "origin": "https//ebank.pingan.com.cn",
                "sec-fetch-site": "same-origin",
                "sec-fetch-mode": "cors",
                "sec-fetch-dest": "empty",
                "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
                "accept-encoding": "gzip, deflate, br",
                "accept-language": "zh-CN,zh;q=0.9",
                "cookie": "WEBTRENDS_ID=2143a2a868e936ec8091661310781821; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHmbEvCoJgJ6uovdqduWl0zvJ-a6UMZB; WT-FPC=id=2143a2a868e936ec8091661310781821lv=1661757231769ss=1661757218522fs=1661310781821pn=7vn=3; BSFIT4_EXPIRATION=1661838937956; BSFIT4_DEVICEID=g8FY5G6z6QRgNFkhFvHyTgPLPOb8g_bEKOL0ckxrplveiPqIA6pG71I2aNM2OGY1c6sdB54fi7hxSKxLqmrbPkhxfdqEdMcDeQsVfjAahc_kzC7yJASKkAGIt5viwrKM92jhNOx1yb-deWCc_LfNefay98HrSTRo"
            },
            'row_mapping': lambda row: {
                FIELD_MAPPINGS['起购金额']: row.get('minAmount', '') + '元',
                FIELD_MAPPINGS['风险等级']: ROW_MAPPING_COFNIG['对公结构性存款']['riskLevel'][row.get('riskLevel', '')],
                FIELD_MAPPINGS['产品名称']: row.get('prdName', ''),
                FIELD_MAPPINGS['管理人']: row.get('manageBankName', ''),
                FIELD_MAPPINGS['产品状态']: ROW_MAPPING_COFNIG['对公结构性存款']['saleStatus'][row.get('saleStatus', '')],
                FIELD_MAPPINGS['产品编码']: row.get('prdCode', ''),
                FIELD_MAPPINGS['收益类型']: ROW_MAPPING_COFNIG['对公结构性存款']['rateType'][
                    row.get('rateType', '')],
                FIELD_MAPPINGS['TA编码']: row.get('taCode', ''),
                FIELD_MAPPINGS['销售渠道']: row.get('tranBankSet', ''),
                FIELD_MAPPINGS['登记编码']: row.get('productNo', '')
            },
        },
    '个人结构性存款': {
        'method': 'post',
        'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
        'data': lambda page_num: {
            'tableIndex': 'table06',
            'dataType': '06',
            'tplId': 'tpl06',
            'pageNum': f'{page_num}',
            'pageSize': '10',
            'channelCode': 'C0002',
            'access_source': 'PC',
        },
        'headers': {
            "host": "ebank.pingan.com.cn",
            "content-length": "99",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
            "accept": "*/*",
            "origin": "https//ebank.pingan.com.cn",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9",
            "cookie": "WEBTRENDS_ID=2143a2a868e936ec8091661310781821; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHmbEvCoJgJ6uovdqduWl0zvJ-a6UMZB; WT-FPC=id=2143a2a868e936ec8091661310781821lv=1661757231769ss=1661757218522fs=1661310781821pn=7vn=3; BSFIT4_EXPIRATION=1661838937956; BSFIT4_DEVICEID=g8FY5G6z6QRgNFkhFvHyTgPLPOb8g_bEKOL0ckxrplveiPqIA6pG71I2aNM2OGY1c6sdB54fi7hxSKxLqmrbPkhxfdqEdMcDeQsVfjAahc_kzC7yJASKkAGIt5viwrKM92jhNOx1yb-deWCc_LfNefay98HrSTRo"
        },
    },
    '公募基金': {
        'method': 'post',
        'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
        'data': lambda page_num: {
            'tableIndex': 'table02',
            'dataType': '02',
            'tplId': 'tpl02',
            'pageNum': f'{page_num}',
            'pageSize': '10',
            'channelCode': 'C0002',
            'access_source': 'PC',
        },
        'headers': {
            "host": "ebank.pingan.com.cn",
            "content-length": "99",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
            "accept": "*/*",
            "origin": "https//ebank.pingan.com.cn",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9",
            "cookie": "WEBTRENDS_ID=2143a2a868e936ec8091661310781821; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHmbEvCoJgJ6uovdqduWl0zvJ-a6UMZB; WT-FPC=id=2143a2a868e936ec8091661310781821lv=1661757231769ss=1661757218522fs=1661310781821pn=7vn=3; BSFIT4_EXPIRATION=1661838937956; BSFIT4_DEVICEID=g8FY5G6z6QRgNFkhFvHyTgPLPOb8g_bEKOL0ckxrplveiPqIA6pG71I2aNM2OGY1c6sdB54fi7hxSKxLqmrbPkhxfdqEdMcDeQsVfjAahc_kzC7yJASKkAGIt5viwrKM92jhNOx1yb-deWCc_LfNefay98HrSTRo"
        },
    },
    '理财子': {
        'method': 'post',
        'url': 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do',
        'data': lambda page_num: {
            'tableIndex': '',
            'dataType': '07',
            'tplId': '',
            'pageNum': f'{page_num}',
            'pageSize': '10',
            'channelCode': 'C0002',
            'access_source': 'PC',
        },
        'headers': {
            "host": "ebank.pingan.com.cn",
            "content-length": "99",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36",
            "content-type": "application/x-www-form-urlencoded;charset=utf-8",
            "accept": "*/*",
            "origin": "https//ebank.pingan.com.cn",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "referer": "https//ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9",
            "cookie": "WEBTRENDS_ID=2143a2a868e936ec8091661310781821; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHmbEvCoJgJ6uovdqduWl0zvJ-a6UMZB; WT-FPC=id=2143a2a868e936ec8091661310781821lv=1661757231769ss=1661757218522fs=1661310781821pn=7vn=3; BSFIT4_EXPIRATION=1661838937956; BSFIT4_DEVICEID=g8FY5G6z6QRgNFkhFvHyTgPLPOb8g_bEKOL0ckxrplveiPqIA6pG71I2aNM2OGY1c6sdB54fi7hxSKxLqmrbPkhxfdqEdMcDeQsVfjAahc_kzC7yJASKkAGIt5viwrKM92jhNOx1yb-deWCc_LfNefay98HrSTRo"
        },
    },
}

ROW_MAPPING_COFNIG = {
    '银行理财': {
        'riskLevel': {
            '3': '中等风险',
            '2': '中低风险',
            '': ''
        },
        'rateType': {
            'FF': '非保本浮动收益',
            '': ''
        },
        'saleStatus': {
            '0': '在售',
            '1': '存续',
            '': ''
        },
    },
    '银行公司理财': {
        'saleStatus': {
            '2': '存续',
            '': ''
        },
        'riskLevel': {
            '2': '中低风险',
            '3': '中等风险',
            '': ''
        },
        'rateType': {
            'FF': '非保本浮动收益',
            '': ''
        }

    },
    '对公结构性存款': {
        'saleStatus': {
            '1': '在售',
            '2': '存续',
            '': ''
        },
        'riskLevel': {
            '1': '低等风险',
            '2': '中低风险',
            '': ''
        },
        'rateType': {
            'BF': '保本浮动收益',
            '': ''
        }
    },
    '公募基金': {
        'riskLevel': {
            '1': '低等风险',
            '2': '中低风险',
            '3': '中等风险',
            '4': '中高风险',
            '5': '高等风险',
            '': ''
        },
        'prdType': {
            '02': '公募',
            '': ''
        },
        'status': {
            'a': '产品终止',
            '0': '开放期',
            '4': '停止交易',
            '5': '停止申购',
            '9': '产品封闭期',
            '': ''
        },

    },
    '理财子': {
        'saleStatus': {
            '': ''
        },
        'riskLevel': {
            '1': '低等风险',
            '2': '中低风险',
            '3': '中等风险',
            '': ''
        }
    }
}
