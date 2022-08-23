const baseUrl = 'https://www.hxb.com.cn/precious-metal-mall/LchqController/findPage?'


function process_data(data) {
    rows = []
    row = {}
    $.each(data, function (index, item) {
        //标题a标签ID赋值
        var daiSpan = "";
        if (item.licaiIssuerLabel === 1) {  //判断licaiIssuerLabel是否为1
            row.daiSpan = '代销华夏理财'
        }
        row.prdName = item.licaiName
        row.prdCode = item.licaiCode
        //产品收益
        var boxNum = "";
        var number = "";

        item.licaiModel = parseInt(item.licaiModel)
        item.licaiNewNet = parseFloat(item.licaiNewNet)
        if (item.licaiModel === 8 || item.licaiModel === 10) {
            number = (item.licaiNewNet * 1).toFixed(4)
            row.dwjz = number
        } else {
            if (item.licaiModel === 6) {
                if (!item.licaiVieldMiddle && item.licaiVieldMiddle !== 0) {
                    number = (item.licaiVieldLow * 100).toFixed(2) + "%~" + (item.licaiVieldMax * 100).toFixed(2);
                } else {
                    number = (item.licaiVieldLow * 100).toFixed(2) + "/" + (item.licaiVieldMiddle * 100).toFixed(2) + "/" + (item.licaiVieldMax * 100).toFixed(2);
                }
                row.yqnhsyl = number //预期年化收益率
            } else if (item.licaiModel === 4) {
                number = (item.licaiVieldMax * 100).toFixed(2);
                row.yjjz = number //业绩基准
            } else if (item.licaiModel === 11) {
                number = (item.currencyVield * 100).toFixed(2);
                row.qrnhsyl = number + '%' //7日年化收益率
            } else {
                number = (item.licaiVieldMax * 100).toFixed(2);
                row.yqzgnhsyl = number
            }
        }
        //期限
        var limitTime = "";
        var limitBox = "";
        if (!item.licaiTimeLimit) {
            row.qx = '无固定期限'
        } else {
            if (item.licaiModel === 2 || item.licaiModel === 3 || item.licaiModel === 8 || item.licaiModel === 9 || item.licaiModel === 10 || item.licaiModel === 11) {
                if (item.licaiTimeLimit.indexOf(0) !== -1 || item.licaiTimeLimit.indexOf(1) !== -1 || item.licaiTimeLimit.indexOf(2) !== -1 || item.licaiTimeLimit.indexOf(3) !== -1 || item.licaiTimeLimit.indexOf(4) !== -1 || item.licaiTimeLimit.indexOf(5) !== -1 || item.licaiTimeLimit.indexOf(6) !== -1 || item.licaiTimeLimit.indexOf(7) !== -1 || item.licaiTimeLimit.indexOf(8) !== -1 || item.licaiTimeLimit.indexOf(9) !== -1) {
                    limitTime = item.licaiTimeLimit + "天"
                } else {
                    limitTime = item.licaiTimeLimit
                }
            } else {
                limitTime = item.licaiTimeLimit + "天"
            }
            row.qx = limitTime
        }
        //发售日期
        if (item.buyBeginDate) {
            if (item.buyEndDate) {
                var dateBox = item.buyBeginDate.substring(0, 4) + "." + item.buyBeginDate.substring(4, 6) + "." + item.buyBeginDate.substring(6, 8) + "至" + item.buyEndDate.substring(0, 4) + "." + item.buyEndDate.substring(4, 6) + "." + item.buyEndDate.substring(6, 8);

            } else if (item.licaiExpireDay1) {
                var dateBox = item.buyBeginDate.substring(0, 4) + "." + item.buyBeginDate.substring(4, 6) + "." + item.buyBeginDate.substring(6, 8) + "至" + item.licaiExpireDay1.substring(0, 4) + "." + item.licaiExpireDay1.substring(4, 6) + "." + item.licaiExpireDay1.substring(6, 8);

            }
        }
        row.fsrq = dateBox
        //起购金额
        row.qgje = item.personalFirstBuyLimit + '万元起'
        //购买渠道
        item.licaiChannelSource = item.licaiChannelSource.trim()
        var sourceName = "";
        if (!item.licaiChannelSource && item.licaiChannelSource !== 0) {
            sourceName = "";
        } else {
            if (item.licaiChannelSource.indexOf("-1") !== -1) {
                sourceName = sourceName + "全部、"
            }
            if (item.licaiChannelSource.indexOf("0") !== -1) {
                sourceName = sourceName + "柜面、"
            }
            if (item.licaiChannelSource.indexOf("1") !== -1 && item.licaiChannelSource !== '-1') {
                sourceName = sourceName + "电话、"
            }
            if (item.licaiChannelSource.indexOf("2") !== -1) {
                sourceName = sourceName + "网银、"
            }
            if (item.licaiChannelSource.indexOf("3") !== -1) {
                sourceName = sourceName + "手机、"
            }
            if (item.licaiChannelSource.indexOf("4") !== -1) {
                sourceName = sourceName + "支付融资、"
            }
            if (item.licaiChannelSource.indexOf("B") !== -1) {
                sourceName = sourceName + "智能柜台、"
            }
            if (item.licaiChannelSource.indexOf("C") !== -1) {
                sourceName = sourceName + "微信银行、"
            }
            if (item.licaiChannelSource.indexOf("8") !== -1) {
                sourceName = sourceName + "e社区、"
            }
            sourceName = sourceName.substring(0, sourceName.length - 1)
        }
        row.gmqd = sourceName
        //发行机构
        var organBox = "";
        if (item.licaiIssuerLabel === "1") {
            organBox = '华夏理财有限责任公司'
        } else {
            organBox = '华夏银行股份有限公司'
        }
        row.fxjg = organBox
        //理财说明书
        var hrefBox = "";
        if (item.href != null) {
            if (item.remark1 === "html") {
                hrefBox = item.href + "." + item.remark1
            } else {
                hrefBox = '/lcpdf/" + item.href + ".pdf'
            }
        }
        row.lcsms = hrefBox
        rows.push(row)
    })
    return rows
}

const doRequest = async (pageNum, pageSize, body) => {
    let response = await fetch(baseUrl + new URLSearchParams({'pageNum': pageNum, 'pageSize': pageSize}),
        {
            headers: {
                'content-type': 'application/json'
            },
            body: JSON.stringify(body),
            method: 'post'
        }
    );
    return await response.json()
};
const body = {
    'licaiBuyOriginOrderFlag': '',
    'licaiName': '',
    'licaiTimeLimitMaxOrderFlag': '',
    'licaiVieldMaxOrderFlag': ''
}
const get_total_count = async () => {
    //获取总页数
    let response = await doRequest(1, 10, body);
    return JSON.stringify(response['body']['total'])
}

const get_target_page = async (pageNum, pageSize) => {
    let response = await doRequest(pageNum, pageSize, body)
    let resp_body = response.body.list
    return JSON.stringify(process_data(resp_body))
}
window.get_total_count = get_total_count
window.get_target_page = get_target_page