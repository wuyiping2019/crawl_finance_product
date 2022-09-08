LOG_NAME = '中国民生银行'
MASK_STR = 'zgmsyh'
TARGET_TABLE = 'ip_bank_zgmsyh_personal'
REQUEST_URL = 'http://www.cmbc.com.cn/xmhpd/grkh/rmcp/rmlc/index.htm'
# 写入数据之前是否删除表
DROP_TABLE = True
# 字段映射
FIELD_MAPPINGS = {
    '产品名称': 'cpmc',
    '产品简称': 'cpjc',
    '货币类型': 'bz',
    '起购金额': 'qgje',
    '风险等级': 'fxdj',
    '募集起始日期': 'mjqsrq',
    '募集结束日期': 'mjjsrq',
    '产品额度': 'cped',
    '剩余额度': 'syed',
    '业绩比较基准': 'yjbjjz',
    '下一个开放日': 'xygkfr',
    '产品起始日期': 'cpqsrq',
    '产品结束日期': 'cpjsrq',
    '运作模式': 'yzms',
    '净值': 'jz',
    '净值日期': 'jzrq',
    '管理人': 'glr',

}
