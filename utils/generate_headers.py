import json

from utils.string_utils import remove_space

headers_str = """
Accept: application/json, text/javascript, */*; q=0.01
Accept-Encoding: gzip, deflate
Accept-Language: zh-CN,zh;q=0.9,en;q=0.8
Cache-Control: no-cache
Content-Length: 261
Content-Type: application/json;charset=UTF-8
Host: www.cmbc.com.cn
Origin: http://www.cmbc.com.cn
Pragma: no-cache
Proxy-Connection: keep-alive
Referer: http://www.cmbc.com.cn/xmhpd/grkh/rmcp/rmlc/index.htm
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36
X-Requested-With: XMLHttpRequest
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
