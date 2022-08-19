-- 创建招商银行个人理财产品表
drop table ip_bank_abc_personal;
CREATE TABLE ip_bank_abc_personal
(
    id                          number(11),
    logId                       number(11),
    collectionMethod            varchar2(100),
    IsCanBuy                    varchar2(1000),
    ProdClass                   varchar2(1000),
    ProdYildType                varchar2(1000),
    ProdLimit                   varchar2(1000),
    ProdProfit                  varchar2(1000),
    PurStarAmo                  varchar2(1000),
    ProdArea                    varchar2(1000),
    ProdSaleDate                varchar2(1000),
    PrdYildTypeOrder            varchar2(1000),
    ProductNo                   varchar2(1000),
    ProdName                    varchar2(1000),
    issuingOffice               varchar2(1000),
    yjjzIntro                   varchar2(1000),
    szComDat                    varchar2(1000),
    createTime                  varchar2(100)
);
-- 创建日志表的序列（用于主键自增）
--drop sequence seq_cmb;
create sequence seq_abc
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
--创建触发器
create or replace trigger trigger_abc
    before insert
    on ip_bank_abc_personal
    for each row
begin
    select seq_abc.nextval into :new.id from dual;
end;

truncate table ip_bank_abc_personal;


insert into ip_bank_abc_personal(PrdName) values ('ceshi');
select * from ip_bank_abc_personal;
