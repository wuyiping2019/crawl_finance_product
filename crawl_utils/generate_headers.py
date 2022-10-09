import json

from crawl_utils.string_utils import remove_space

headers_str = """
Host: www.zhihu.com
Connection: keep-alive
x-zse-93: 101_3_3.0
x-ab-param: 
x-ab-pb: CsgBGwA/AEcAtABpAWoBdAE7AswC1wLYAk8DUAOgA6EDogO3A/MD9AMzBIwEjQSmBNYEEQUyBVEFiwWMBZ4FMAYxBn4G6wYnB3cHeAfYB9wH3QdnCHQIdgh5CMUI2gg/CUIJYAmNCcMJxAnFCcYJxwnICckJygnLCcwJ0QnxCfQJBApJCmUKawqYCqUKqQq+CsQK1ArdCu0K/Qr+CjsLPAtDC0YLcQt2C4ULhwuNC8AL1wvgC+UL5gssDDgMcQyPDKwMuQzDDMkM+AwSZAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=
x-requested-with: fetch
x-zse-96: 2.0_B2rH/jsGQNYNYhVk0GbkTH2YRmU11pr3Ndty0BXU3f8g6IrOymNdqszzorzsLrFq
User-Agent: Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36
x-zst-81: 3_2.0VhnTj77m-qofgh3TxTnq2_Qq2LYuDhV80wSL7T9yr6nxeTtqXRFZSTrZgcO9kUS_IHkmWgLBpukYWgxmIQPYqbpfZUnPS0FZG0Yq-6C1ZhSYVCxm1uV_70RqSMCBVuky16P0riRCyufOovxBXgSCiiR0ELYuUrxmtDomqU7ynXtOnAoTh_PhRDSTF4HBJ9HLsBOYTwOLNq2Kbu2YFwgqzuxKVCFLpcHKHCF1EJCO6GLfQUH9fXeTvHw12reBkMCfKic1hJVZSG3me9V_XJHGgg31bwXKoXeMt9F16AXCUDcubgxfjDLKxD9mArSshgpOzUt9YU3qLucXzwNMtgpCoqcBQrNMn9Ym3Cg_YCXmVqkwuhcMggH0Vbx0Irx89vXG_UCyeJxYS8pfVDOKN9NMPbC1hUOGz9L8Qqc_DgxYIgVBeT21uCwqovof29LKbAXCUC2YUcLGDqgLbwgMp9XYeMeBWhHM2rSC
Accept: */*
Origin: https://zhuanlan.zhihu.com
Sec-Fetch-Site: same-site
Sec-Fetch-Mode: cors
Sec-Fetch-Dest: empty
Referer: https://zhuanlan.zhihu.com/p/27608348
Accept-Encoding: gzip, deflate, br
Accept-Language: zh-CN,zh;q=0.9
Cookie: _zap=c327c9ce-9c84-4329-bac7-79fd9b475c76; d_c0="APDQlw0SHRKPTqZqOrsE3dj8HZHtTxjovZE=|1603968534"; _xsrf=3SmcSgji3W2HtHLn3iIcdwBiPvl04WDi; _9755xjdesxxd_=32; YD00517437729195%3AWM_TID=zDc5XyFbzL1FQQBRVVY7nFKogSBzCOiH; YD00517437729195%3AWM_NI=JARFs734My9fIxWZHhfahsRXiDT5ug7fD1ZfZEcet5Hp2GomzEnImq5NxWfk5t8Nudwp%2BmZ8ENkPFqY6Ul5TOdDHqHIpH6%2BCg5sHuK0Vha2vqJ%2F0OfxTuqmrTVeKjJu7em0%3D; YD00517437729195%3AWM_NIKE=9ca17ae2e6ffcda170e2e6eea8f368b1bd9ddab67eb4928bb2c55b839a9f86d15dba90a993d141aca8bd82cd2af0fea7c3b92a8a9f9ea7f23df1ec839abc6e8ba899bbcb64a6bfa894d154ab9c9cd6f86df5ba8cd2d96e9a9bad86f73f8cb0fea8ee67ba9ba2aaf07bf792b8b8e561f7acf8aff870b5b0e58ed9348792a5b1c57b9bea9b8db8608dbc8ca9f4448bb088d7aa49a5ebbda9f841f48aa882b17eb396e1d1ce6ebc8cbb92f07a988aabace942b7e9ab8eb337e2a3; gdxidpyhxdE=UTT43Heojmg%5CjScYgzOfVeEP35mzE8rI%5CM8yOJIw9E%5CM%2BLfGbeHsD3U%2FhVOMSqjfAw9%2FJMCsEsKUvDk0jCQfN5bEGIhzPDATshbpUO%2F0ggqReKjH0rDNYVoaanMyt3cqp6uCJAj8c%2BEsqTMxIDN1K7%2FcSic5SKdZMUSWSEH45GA%5CY%2Br6%3A1663723913499; Hm_lvt_98beee57fd2ef70ccdd5ca52b9740c49=1663057627,1663234404,1663637981,1663748189; captcha_session_v2=2|1:0|10:1664439040|18:captcha_session_v2|88:a04rRHdSWVAzZnZHTWw0bmlCWktSOGZjRDY4Q1VYeFJIT3YyVzN5Vk12MjNTQlZCUFF1UkV1ZVloQjNzQ3FJdA==|3479878ea13ce2530d023aa27acc88d0dfdab6023dfb9a52972dfab35853c00a; KLBRSID=57358d62405ef24305120316801fd92a|1664441728|1664439040



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
