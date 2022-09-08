const getData = async (pageNo, pageSize = 10) => {
    params.pageNo = pageNo;
    return JSON.stringify(await $gw_poweb('QryProdListOnMarket', params, (res) => {
        res.pageSize = pageSize
        res.pageNo = pageNo
        let incomeInfos = []
        res.prdList.forEach(item => {
            let nextOpenDate = getNextOpenDates(item)
            let itemInfo = getFinanceProductIncome(item);
            itemInfo.nextOpenDate = nextOpenDate
            incomeInfos.push(itemInfo)
        })
        res.incomeInfos = incomeInfos
        return res;
    }));
}

window.getData = getData