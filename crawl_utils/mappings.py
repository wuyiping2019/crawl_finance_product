from crawl_utils.common_utils import delete_empty_value

FIELD_MAPPINGS = {
    '产品名称': 'cpmc',
    '产品编码': 'cpbm',
    '产品简称': 'cpjc',
    '币种': 'bz',
    '起购金额': 'qgje',
    '递增金额': 'dzje',
    '风险等级': 'fxdj',
    '募集起始日期': 'mjqsrq',
    '募集结束日期': 'mjjsrq',
    '产品额度': 'cped',
    '剩余额度': 'syed',
    '业绩比较基准': 'yjbjjz',
    '下一个开放日': 'xygkfr',
    '产品起始日期': 'cpqsrq',
    '产品结束日期': 'cpjsrq',
    '发售起始日期': 'fsqsrq',
    '发售截止日期': 'fsjzrq',
    '产品到期日': 'cpdqr',
    '运作模式': 'yzms',
    '净值': 'jz',
    '净值日期': 'jzrq',
    '管理人': 'glr',
    '管理人编码': 'glrbm',
    '管理人名称': 'glrmc',
    '总额度': 'zed',
    '销售渠道': 'xsqd',
    '产品状态': 'cpzt',
    '委托人': 'wtr',
    '委托人编码': 'wtrbm',
    '历史净值': 'lsjz',
    '投资期限': 'tzqx',
    '登记编码': 'djbm',
    '募集方式': 'mjfs',  # 商业银行应当根据募集方式的不同，将理财产品分为公募理财产品和私募理财产品。
    '投资性质': 'tzxz',  # 商业银行应当根据投资性质的不同，将理财产品分为固定收益类理财产品、权益类理财产品、商品及金融衍生品类理财产品和混合类理财产品。
    '起息日': 'qxr',
    '预计到期利率': 'yjdqlv',
    '收益类型': 'sylx',
    '成立日': 'clr',
    '开放日': 'kfr',
    '销售区域': 'xsqy',
    'TA编码': 'tabm',
    'TA名称': 'tamc',
    '发行价': 'fxj',
    '购买规则': 'gmgz',
    '赎回规则': 'shgz',
    '收益规则': 'sygz',
    '业务分类': 'ywfl',  # 根据爬取页面的分类标准进行的分类
    '分红方式': 'fhfs',
    '产品说明书': 'cpsms',
    '产品合约': 'cphy',
    '风险揭示书': 'fxjss',
    '产品收益结构图': 'cpsyjgt'
}


def row_mapping(field_mapping_fields: list, origin_row_fields: list, origin_row: dict):
    row = {}
    for mapping_field, origin_field in zip(field_mapping_fields, origin_row_fields):
        row[FIELD_MAPPINGS[mapping_field]] = str(
            origin_row.get(origin_field, '') if origin_row.get(origin_field, '') else '')
    delete_empty_value(row)
    return row
