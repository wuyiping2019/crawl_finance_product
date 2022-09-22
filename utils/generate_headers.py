import json

from utils.string_utils import remove_space

headers_str = """
host: wap.cgbchina.com.cn
accept: application/json, text/plain, */*
origin: https://wap.cgbchina.com.cn
sendersn: 1663823532558n1005981
user-agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x6700143B) WindowsWechat(0x6307062c)
content-type: application/json;charset=UTF-8
sec-fetch-site: same-origin
sec-fetch-mode: cors
sec-fetch-dest: empty
referer: https://wap.cgbchina.com.cn/h5-mobilebank-web/h5/investment/self/detail_repeat_1663817605945?srcChannel=WX&HMBA_STACK_HASH=1663748433050&prdCode=XFTLCY0601ZSA&mainChannel=400&secondaryChannel=WX&ecifNum=&sourceActivityId=&cusmanagerId=&srcEcifNum=&accOpenNode=&channel=400&sChannel=MB&isRegistCS=1&faceFlag=LS&trade_BranchNo=&loginType=app&signType=&guideBanner=
accept-encoding: gzip, deflate, br
accept-language: zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7
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
