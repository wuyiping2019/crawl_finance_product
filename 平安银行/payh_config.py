from enum import Enum

from utils.common_utils import delete_empty_value
from utils.custom_exception import CustomException
from utils.mappings import FIELD_MAPPINGS

# 缺失编码记录
MISSING_CODE_SAMPLE_COUNT = 3


class CrawlRequestParams(Enum):
    data = 'data'
    json = 'json'
    params = 'params'
    url = 'url'
    headers = 'headers'
    method = 'method'


class CrawlConfig:
    def __init__(self, requests, row_mapping_config, missing_code_sample_count=None, missing_code_log=None):
        """
        :param requests: 保存请求参数
        :param row_mapping_config: 保存映射配置
        :param missing_code_sample_count: 配置需要保存映射配置缺失样本数据条数
        :param missing_code_log: 保存映射配置确实的缺失记录
        """
        self.requests = requests
        self.row_mapping_config = row_mapping_config
        self.missing_code_sample_count = missing_code_sample_count if missing_code_sample_count is None else 3
        self.missing_code_log = missing_code_log if missing_code_log is None else {}

    def get_request(self, key, **kwargs) -> dict:
        request = self.requests[key]
        params = request.get(CrawlRequestParams.params)
        data = request.get(CrawlRequestParams.data)
        json = request.get(CrawlRequestParams.json)
        headers = request.get(CrawlRequestParams.headers)
        url = request.get(CrawlRequestParams.url)
        method = request.get(CrawlRequestParams.method)
        request = {
            'params': params,
            'data': data,
            'json': json,
            'headers': headers,
            'url': url,
            'method': method
        }
        for k, v in kwargs.items():
            request[k] = v
        delete_empty_value(request)
        return request

    def __row_mapping(self):

    # decode_field_coding(row, '理财子', 'saleStatus')
    def decode_field_coding(self, row: dict, row_mapping_config_key: str, code_prop: str):
        """
        :param row: 获取的原始数据 是一个dict
        :param row_mapping_config_key: row_mapping_config中的key
        :param code_prop: row中编码的属性
        :return:
        """
        # 某一个请求的映射字段 如理财子
        mapping: dict = ROW_MAPPING_COFNIG[row_mapping_config_key]
        # 某一个属性的映射 如saleStatus
        code_mapping: dict = mapping[code_prop]
        # 从原始数据中获取的编码数据
        code = row.get(code_prop, '')
        # 如果配置的编码映射中不存在该code
        if code not in code_mapping.keys():
            if row_mapping_config_key not in self.missing_code_log.keys():
                self.missing_code_log[row_mapping_config_key] = {}
            if code_prop not in self.missing_code_log[row_mapping_config_key].keys():
                self.missing_code_log[row_mapping_config_key][code_prop] = {}
            if code not in self.missing_code_log[row_mapping_config_key][code_prop].keys():
                self.missing_code_log[row_mapping_config_key][code_prop][code] = []
            else:
                if len(self.missing_code_log[row_mapping_config_key][code_prop][code]) <= MISSING_CODE_SAMPLE_COUNT:
                    self.missing_code_log[row_mapping_config_key][code_prop][code].append(row)
            return code
        else:
            return code_mapping[code]
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
        'row_mapping': lambda row: {
            FIELD_MAPPINGS['起购金额']: row.get('minAmount', '') + '元',
            FIELD_MAPPINGS['收益类型']: ROW_MAPPING_COFNIG['个人结构性存款']['rateType'][
                row.get('rateType', '')],
            FIELD_MAPPINGS['风险等级']: ROW_MAPPING_COFNIG['个人结构性存款']['riskLevel'][row.get('riskLevel', '')],
            FIELD_MAPPINGS['产品名称']: row.get('prdName', ''),
            FIELD_MAPPINGS['产品状态']: ROW_MAPPING_COFNIG['个人结构性存款']['saleStatus'][row.get('saleStatus', '')],
            'mobile_url': row.get('prdDetailUrl', ''),
            FIELD_MAPPINGS['产品编码']: row.get('prdCode', ''),
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
        'row_mapping': lambda row: {
            # FIELD_MAPPINGS['产品编码']:row.get('productSeriesFont',''),
            FIELD_MAPPINGS['风险等级']: ROW_MAPPING_COFNIG['公募基金']['riskLevel'][row.get('riskLevel', '')],
            FIELD_MAPPINGS['产品名称']: row.get('prdName', ''),
            'mobile_url': row.get('prdDetailUrl', ''),
            FIELD_MAPPINGS['产品编码']: row.get('prdCode', ''),
            'pc_detail_url': row.get('prdPcDetailUrl', ''),
            FIELD_MAPPINGS['投资性质']: row.get('prdArrName', ''),
            FIELD_MAPPINGS['TA编码']: row.get('taCode', ''),
            FIELD_MAPPINGS['TA名称']: row.get('taName', ''),
            FIELD_MAPPINGS['募集方式']: ROW_MAPPING_COFNIG['公募基金']['prdType'][row.get('prdType', '')],
            FIELD_MAPPINGS['管理人']: row.get('prdManager', ''),
            FIELD_MAPPINGS['产品状态']: ROW_MAPPING_COFNIG['公募基金']['status'][row.get('status', '')]

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
        'row_mapping': lambda row: {
            # FIELD_MAPPINGS['产品编码']:row.get('productSeriesFont',''),
            FIELD_MAPPINGS['风险等级']: decode_field_coding(row, '理财子', 'riskLevel'),
            FIELD_MAPPINGS['产品名称']: row.get('prdName', ''),
            FIELD_MAPPINGS['产品状态']: decode_field_coding(row, '理财子', 'saleStatus'),
            'mobile_url': row.get('prdDetailUrl', ''),
            'pc_detail_url': row.get('prdPcDetailUrl', ''),
            FIELD_MAPPINGS['产品编码']: row.get('prdCode', ''),
            FIELD_MAPPINGS['TA名称']: row.get('taName', ''),
            FIELD_MAPPINGS['管理人']: row.get('prdManager', '')
        },
    },

}
CrawlConfig()

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


