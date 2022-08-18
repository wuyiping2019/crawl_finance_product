const fs = require("fs")
const jsFilePath = process.argv.slice(2)[0]
let fileStr = fs.readFileSync(jsFilePath, 'UTF8');
const jQuery1113030268752832306167_1660787492286 = (data) => {
    return {
        'totalCount': data.totalCount,
        'pageCount': data.pageCount,
        'retCode': data.retCode,
        'retMsg': data.retMsg,
        'resultList': data.content.resultList
    }
}

let resp_object = eval(fileStr);
let json_str = JSON.stringify(resp_object);

console.log(json_str)





