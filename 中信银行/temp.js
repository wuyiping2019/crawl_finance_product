const fundQryBaseConfig = () => fetch(
    'https://m1.cmbc.com.cn/gw/m_app/FinFundDetailInfo.do',
    {
        headers: {
            'Content-Type': 'application/json;charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Mobile Safari/537.36'
        },
        data: {
            "header": {
                "appId": "",
                "appVersion": "web",
                "appExt": "999",
                "device": {
                    "appExt": "999",
                    "osType": "03",
                    "osVersion": "",
                    "uuid": ""
                }
            },
            "body": {
                "prdCode": "2119187301A"
            }
        }
    }
).then(resp => resp.text())
window.fundQryBaseConfig = fundQryBaseConfig