# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field


class ZGLCWItem(scrapy.Item):
    logId = Field()
    # 来自中国理财网的字段
    djbm = Field()  # 登记编码
    fxjg = Field()  # 发行机构
    zyms = Field()  # 运作模式（开放式非净值型）
    mjfs = Field()  # 募集方式
    qxlx = Field()  # 期限类型（1 - 3个月）
    mjbz = Field()  # 募集币种（人民币（CNY））
    tzxz = Field()  # 投资性质
    fxdj = Field()  # 风险等级
    mjqsrq = Field()  # 募集起始日期
    mjjsrq = Field()  # 募集结束日期
    cpqsrq = Field()  # 产品起始日期
    cpjsrq = Field()  # 产品结束日期
    ywqsr = Field()  # 业务起始日
    ywjsr = Field()  # 业务结束日
    sjts = Field()  # 实际天数
    csjz = Field()  # 初始净值
    cpjz = Field()  # 产品净值
    ljjz = Field()  # 累计净值
    zjycdfsyl = Field()  # 最近一次兑付收益率
    cptssx = Field()  # 产品特殊属性
    tzzclx = Field()  # 投资资产类型
    dxjg = Field()  # 代销机构
    yqzdsyl = Field()  # 预期最低收益率
    yqzgsyl = Field()  # 预期最高收益率
    yjbjjz = Field()  # 业绩比较基准
    xsqy = Field()  # 销售区域


class PayhItem(scrapy.Item):
    id = Field()
    logId = Field()
    djbm = Field()
    cpmc = Field()  # --产品名称
    cpbm = Field()  # 产品编码
    tadm = Field()  # td代码
    qgje = Field()  # 起购金额
    xswd = Field()  # 销售网点
    cpzt = Field()  # 产品状态
    cplx = Field()  # 产品类型(非保本浮动收益)
    cpfl = Field()  # 产品分类（平安银行自己的分类（公募基金、理财子、公司代销理财产品等））
    cply = Field()  # 产品来源（平安银行）
    cpgs = Field()  # 产品归属（本行产品、代销产品）
    createTime = Field()  # 创建时间
