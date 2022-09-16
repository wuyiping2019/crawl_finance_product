import json

from utils.string_utils import remove_space

headers_str = """
host: ebank.pingan.com.cn
content-length: 99
user-agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36
content-type: application/x-www-form-urlencoded;charset=utf-8
accept: */*
origin: https://ebank.pingan.com.cn
sec-fetch-site: same-origin
sec-fetch-mode: cors
sec-fetch-dest: empty
referer: https://ebank.pingan.com.cn/aum/common/sales_list/index.html?initPage=true
accept-encoding: gzip, deflate, br
accept-language: zh-CN,zh;q=0.9
cookie: WEBTRENDS_ID=2143a2a868e936ec8091661310781821; fp_ver=4.7.9; BSFIT4_OkLJUJ=FHmbEvCoJgJ6uovdqduWl0zvJ-a6UMZB; WT-FPC=id=2143a2a868e936ec8091661310781821:lv=1661757231769:ss=1661757218522:fs=1661310781821:pn=7:vn=3; BSFIT4_EXPIRATION=1661838937956; BSFIT4_DEVICEID=g8FY5G6z6QRgNFkhFvHyTgPLPOb8g_bEKOL0ckxrplveiPqIA6pG71I2aNM2OGY1c6sdB54fi7hxSKxLqmrbPkhxfdqEdMcDeQsVfjAahc_kzC7yJASKkAGIt5viwrKM92jhNOx1yb-deWCc_LfNefay98HrSTRo
"""
splits = headers_str.split('\n')
headers_dict = {}
for item in splits:
    if item == '':
        continue
    else:
        kv = item.split(':')
        if len(kv) == 2:
            k = kv[0]
            v = kv[1]
            headers_dict[k] = str(v).strip()
        else:
            k = kv[0]
            v = ''.join(kv[1:])
            headers_dict[k] = str(v).strip()
print(json.dumps(headers_dict))

for k, v in headers_dict.items():
    print(f'{k}: {v}')
