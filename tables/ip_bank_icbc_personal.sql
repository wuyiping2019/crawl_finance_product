-- 创建中国工商银行理财产品表
drop table ip_bank_icbc_personal;
CREATE TABLE ip_bank_icbc_personal
(
    id              number(11),
    logId           number(11),
    fxjg            varchar2(100),
    cpzt            varchar2(1000),
    cpmc            varchar2(1000),
    jz              varchar2(1000),
    mwfsy           varchar2(1000),
    cpsy            varchar2(1000),
    qgje            varchar2(1000),
    zdcyqx          varchar2(1000),
    fxdj            varchar2(1000),
    createTime      varchar2(100)
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
