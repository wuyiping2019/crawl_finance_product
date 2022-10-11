import json

from crawl_utils.string_utils import remove_space

headers_str = """
metadata: ProductCode|ProductName|DeadlineBrandiD|IncomeDates|RiskLevel|CurrencyType|IndiiPOMinAmnt|TACode|TAName|ChannelDisincomeRate|IncomeRateDes|Productstatus
channelid: 267467
page: 1
searchword: (DeadlineBrandiD=%)*(IncomeDates=%)*(RiskLevel=%)*(CurrencyType=%)*(IndiIPOMinStauts=%)*(TACode=%)*(ProductStatus='在售' or ProductStatus='停售')*(ProductName!=公司%)*(ProductName!=同业%)*(ProductName!=利多多%)*(ProductName!=财富班车%)*(ProductName!=天添利%)*(ProductName!=启铭%)*(ProductName!=天通利%)*(ProductName!=信托%)*(ProductName!=上信%)*(ProductCode!=2301137335)*(ProductCode!=2301212262)*(ProductCode!=2301228801)*(ProductCode!=2301228803)*(ProductCode!=2301174902)*(ProductCode!=2301174903)*(ProductCode!=2301157376)*(ProductCode!=2301157377)*(ProductCode!=2301229055)
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
