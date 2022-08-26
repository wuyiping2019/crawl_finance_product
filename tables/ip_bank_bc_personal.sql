-- 创建中国银行理财产品表
drop table ip_bank_bc_personal;
CREATE TABLE ip_bank_bc_personal
(
    id                                  number(11),
    logId                               number(11),
    ProductCode                         varchar2(100),
    ProductName                         varchar2(1000),
    ProdLimit                           varchar2(1000),
    ProdProfit                          varchar2(1000),
    PurStarAmo                          varchar2(1000),
    Countertop                          varchar2(1000),
    InternetBank                        varchar2(1000),
    MobileBank                          varchar2(1000),
    SelfTerminal                        varchar2(1000),
    FastCommunication                   varchar2(1000),
    WeChat                              varchar2(1000),
    RiskLevel                           varchar2(1000),
    CollectionStartDate                 varchar2(1000),
    CollectionEndDate                   varchar2(1000),
    InterestCommencementDate            varchar2(1000),
    MaturityDate                        varchar2(1000),
    InvestorsType                       varchar2(1000),
    ProdArea                            varchar2(1000),
    PurchaseInstructions                varchar2(1000),
    RedemptionInstructions              varchar2(1000),
    ProductType                         varchar2(1000),
    createTime                          varchar2(100)
);
-- 创建日志表的序列（用于主键自增）
--drop sequence seq_cmb;
create sequence seq_bc
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
--创建触发器
create or replace trigger trigger_bc
    before insert
    on ip_bank_bc_personal
    for each row
begin
    select seq_bc.nextval into :new.id from dual;
end;

truncate table ip_bank_bc_personal;


insert into ip_bank_abc_personal(PrdName) values ('ceshi');
select * from ip_bank_abc_personal;
