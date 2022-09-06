import json

from utils.string_utils import remove_space

headers_str = """
Accept: application/json, text/plain, */*
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9
Cache-Control: no-cache
Connection: keep-alive
Content-Length: 165
Content-Type: application/json;charset=UTF-8
Cookie: bigipW_http_m1_cookie=!l8rVrFJBj4tRTu2CPONelVKPr9b9hBOOAoWQHv1I7vgp2a/AyjN69SZfEZZnqWxTkR3COROa4Hp3FJ4=; org.springframework.web.servlet.i18n.CookieLocaleResolver.LOCALE=zh_CN; BIGipServerUEB_tongyidianziqudao_app_41002_pool=!St8dgRWkSo/BtTE0lXP1ySZhZOpxgv24jzh9ZaJpR2l/ef9s9cn163PaVT9qwCY3NEzQMC+fkES17Q==; BIGipServershoujiyinhang_geren_app_8000_pool=!51ZeERI+oUoWKGU0lXP1ySZhZOpxgoTX3jEtV4hKefJOXPYWa7W2QA3W5PX/ds4wBejA2iLvoGoWXpk=; RSESSIONID=7BE6EC2A61CB96EEB14E1593D5300490; lastAccessTime=1662000902310; JSESSIONID=VHT2-TOJHJCrBfwBhjkOUW-XLp11aUhnOEG991LJPuK7R6VPN3mu!-1936040846
Host: m1.cmbc.com.cn
Origin: https://m1.cmbc.com.cn
Pragma: no-cache
Referer: https://m1.cmbc.com.cn/CMBC_MBServer/new/app/mobile-bank/fund-commission/details?isZhuanShu=false&ktype=4&newFlag=0&prdCode=22115002M
sec-ch-ua: "Google Chrome";v="95", "Chromium";v="95", ";Not A Brand";v="99"
sec-ch-ua-mobile: ?1
sec-ch-ua-platform: "Android"
Sec-Fetch-Dest: empty
Sec-Fetch-Mode: cors
Sec-Fetch-Site: same-origin
User-Agent: Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36
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

for k,v in headers_dict.items():
    print(f'{k}: {v}')
