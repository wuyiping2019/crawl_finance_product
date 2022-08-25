//理财产品
url = 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do'
//channelCode = C0002
const doPost = async (tableIndex, dataType, tplId, pageNum, pageSize, channelCode, access_source) => {
    let resp = await fetch(url,
        {
            method: 'post',
            body: `tableIndex=${tableIndex}&dataType=${dataType}&tplId=${tplId}&pageNum=${pageNum}&pageSize=${pageSize}&channelCode=${channelCode}&access_source=${access_source}`,
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'
            }
        }
    )
        .then(resp => resp.text())
        .catch(resp => resp);
    return resp
}
window.doPost = doPost