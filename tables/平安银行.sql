--建表语句
drop table ip_bank_payh_personal;
create table ip_bank_payh_personal
(
    id         number(11),     --自增Id
    logId      number(11),     --日志id
    cpmc       varchar2(1000), --产品名称
    cpbm       varchar2(1000), --产品编码
    tadm       varchar2(1000), --td代码

    --来自中国理财网的字段
    djbm       varchar2(1000), --登记编码
    fxjg       varchar2(1000), --发行机构
    zyms       varchar2(1000), --运作模式（开放式非净值型）
    mjfs       varchar2(1000), --募集方式
    qxlx       varchar2(1000), --期限类型（1-3个月）
    mjbz       varchar2(1000), --募集币种（人民币（CNY））
    tzxz       varchar2(1000), --投资性质
    fxdj       varchar2(1000), --风险等级
    mjqsrq     varchar2(1000), --募集起始日期
    mjjsrq     varchar2(1000), --募集结束日期
    cpqsrq     varchar2(1000),--产品起始日期
    cpjsrq     varchar2(1000),--产品结束日期
    ywqsr      varchar2(1000),--业务起始日
    ywjsr      varchar2(1000),--业务结束日
    sjts       varchar2(1000),--实际天数
    csjz       varchar2(1000),--初始净值
    cpjz       varchar2(1000),--产品净值
    ljjz       varchar2(1000),--累计净值
    zjycdfsyl  varchar2(1000),--最近一次兑付收益率
    cptssx     varchar2(1000),--产品特殊属性
    tzzclx     varchar2(1000),--投资资产类型
    dxjg       varchar2(1000),--代销机构
    yqzdsyl    varchar2(1000),--预期最低收益率
    yqzgsyl    varchar2(1000),--预期最高收益率
    yjbjjz     varchar2(1000),--业绩比较基准
    xsqy       clob,           --销售区域

    qgje       varchar2(1000), --起购金额
    xswd       varchar2(1000), --销售网点
    cpzt       varchar2(1000), --产品状态
    cplx       varchar2(1000), --产品类型(非保本浮动收益)

    cpfl       varchar2(1000), --产品分类（平安银行自己的分类（公募基金、理财子、公司代销理财产品等））
    cply       varchar2(1000), --产品来源（平安银行）
    cpgs       varchar2(1000), --产品归属（本行产品、代销产品）
    createTime varchar2(1000)  --创建时间
);
--创建sequence
create sequence seq_payh
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
--创建trigger
create or replace trigger trigger_payh
    before insert
    on ip_bank_payh_personal
    for each row
begin
    select seq_payh.nextval into :new.id from dual;
end;

truncate table ip_bank_payh_personal;
select count(1)
from ip_bank_payh_personal;
select *
from ip_bank_payh_personal
order by createTime desc;
select *
from SPIDER_LOG
order by id desc;

select *
from SPIDER_LOG
order by id desc;

