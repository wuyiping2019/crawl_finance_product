//理财产品
lccp_url = 'https://ebank.pingan.com.cn/rmb/bron/ibank/pop/fund/supervise/qrySuperviseProductList.do'
//channelCode = C0002
const doPostLCCPURL = async (pageNum, pageSize, channelCode) => {
    let resp = await fetch(lccp_url,
        {
            method: 'post',
            body: `tableIndex=table01&dataType=01&tplId=tpl01&pageNum=${pageNum}&pageSize=${pageSize}&channelCode=${channelCode}&access_source=PC`,
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8'
            }
        }
    )
        .then(resp => resp.text())
        .catch(resp => resp);
    return resp
}
window.doPostLCCPURL = doPostLCCPURL