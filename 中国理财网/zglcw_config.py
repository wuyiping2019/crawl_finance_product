MASK = 'zglcw'
LOG_NAME = '中国理财网'
PC_REQUEST_URL = 'https://www.chinawealth.com.cn/LcSolrSearch.go'
PC_REQUEST_METHOD = 'POST'
PC_REQUEST_DATA = lambda page_no:{
    'cpjglb': '',
    'cpyzms': '',
    'cptzxz': '',
    'cpfxdj': '',
    'cpqx': '',
    'mjbz': '',
    'cpzt': '02,04,06',
    'mjfsdm': '01,NA',
    'cpdjbm': '',
    'cpmc': '',
    'cpfxjg': '',
    'yjbjjzStart': '',
    'yjbjjzEnd': '',
    'areacode': '',
    'pagenum': f'{page_no}',
    'orderby': '',
    'code': '',
    'sySearch': '-1',
    'changeTableFlage': '0'
}
PC_REQUEST_HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Content-Length': '157',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Host': 'www.chinawealth.com.cn',
    'Origin': 'https://www.chinawealth.com.cn',
    'Pragma': 'no-cache',
    'Referer': 'https://www.chinawealth.com.cn/zzlc/jsp/lccp.jsp',
    'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}
PC_CITY = {
    '000000': '全国', '820000': '澳门', '340000': '安徽', '110000': '北京', '500000': '重庆', '210200': '大连', '350000': '福建',
    '620000': '甘肃', '440000': '广东', '450000': '广西', '520000': '贵州',
    '410000': '河南', '130000': '河北', '430000': '湖南', '420000': '湖北', '230000': '黑龙江', '460000': '海南', '220000': '吉林',
    '210000': '辽宁', '320000': '江苏', '360000': '江西', '330200': '宁波',
    '150000': '内蒙古', '640000': '宁夏', '310000': '上海', '370000': '山东', '140000': '山西', '440300': '深圳', '510000': '四川',
    '370200': '青岛', '630000': '青海', '610000': '陕西', '900000': '其他国家或地区',
    '120000': '天津', '710000': '台湾', '330000': '浙江', '350200': '厦门', '530000': '云南', '650000': '新疆', '810000': '香港',
    '540000': '西藏'
}
