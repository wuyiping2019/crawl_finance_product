-- 创建中国工商银行理财产品表
drop table ip_bank_icbc_personal;
CREATE TABLE ip_bank_icbc_personal
(
    id                          number(11),
    logId                       number(11),
    IssuingBank            varchar2(100),
    State                    varchar2(1000),
    ProdName                   varchar2(1000),
    UnitNet                   varchar2(1000),
    SharesGain                   varchar2(1000),
    ProdProfit                varchar2(1000),
    PurStarAmo                     varchar2(1000),
    ProdLimit                  varchar2(1000),
    RiskLevel                  varchar2(1000),
    createTime                  varchar2(100)
);
-- 创建日志表的序列（用于主键自增）
--drop sequence seq_icbc;
create sequence seq_icbc
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
--创建触发器
create or replace trigger trigger_icbc
    before insert
    on ip_bank_icbc_personal
    for each row
begin
    select seq_icbc.nextval into :new.id from dual;
end;

truncate table ip_bank_icbc_personal;


insert into ip_bank_icbc_personal(PrdName) values ('ceshi');
select * from ip_bank_icbc_personal;
