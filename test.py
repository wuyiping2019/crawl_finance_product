
import sys
import requests
import json

SERVICE_ADD = 'http://10.2.13.150:8000/kgqa'

# query = input("please input a query:\t")

input_data = {}
# input_data['query'] = query
input_data['query'] = '任宪韶的毕业院校是?'
json_str = json.dumps(input_data).encode().decode('unicode_escape')
print(json_str)

result = requests.post(SERVICE_ADD, json=input_data)
print(result)
res_json = json.loads(result.text)

query = '任宪韶的毕业院校是?'
print("QUERY:\t" + query)
print(res_json)
