import json

from crawl_utils.string_utils import remove_space

headers_str = """
Accept: */*
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9,en;q=0.8
Cache-Control: no-cache
Connection: keep-alive
Host: open.cebbank.com
Pragma: no-cache
Referer: http://www.cebbank.com/
sec-ch-ua: "Google Chrome";v="95", "Chromium";v="95", ";Not A Brand";v="99"
sec-ch-ua-mobile: ?0
sec-ch-ua-platform: "Windows"
Sec-Fetch-Dest: script
Sec-Fetch-Mode: no-cors
Sec-Fetch-Site: cross-site
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36
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
            v = ''.join(kv[1:]).encode().decode('unicode_escape')
            headers_dict[k] = str(v).strip()
print(json.dumps(headers_dict))

for k, v in headers_dict.items():
    print(f'{k}: {v}')
