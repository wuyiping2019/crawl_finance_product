origin_url = "http://www.cmbchina.com/cfweb/svrajax/product.ashx?op=search&type=m&pageindex={{pageindex}}&salestatus=&baoben=&currency=&term=&keyword=&series=01&risk=&city=&date=&pagesize=100&orderby=ord1&t=0.5991545084330754&citycode="

const getTotalInfo = async () => {
    let url = origin_url.replace("{{pageindex}}", '1')
    let response = await fetch(url)
    let data = await response.text(); //并不是json形式的字符串数据
    let obj = eval(data);
    return {
        "totalPage": obj.totalPage,
        "totalRecord": obj.totalRecord
    }
}
const doRequest = async (currentPage) => {
    let url = origin_url.replace("{{pageindex}}", '' + currentPage);
    let response = await fetch(url);
    let data = await response.text();
    let obj = eval(data);
    return obj.list;
};

const getTotalData = async () => {
    const totalInfo = await getTotalInfo();
    totalPage = totalInfo.totalPage;
    totalRecord = totalInfo.totalRecord;
    let currentPage = 1;
    productListRS = []
    while (currentPage <= totalPage) {
        productList = await doRequest(currentPage)
        for (let product of productList) {
            productListRS.push(product)
        }
        currentPage++;
    }
    return JSON.stringify(productListRS)
}
window.getTotalData = getTotalData



