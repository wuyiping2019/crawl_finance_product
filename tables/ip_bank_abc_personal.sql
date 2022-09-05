-- 创建中国农业银行理财产品表
drop table ip_bank_abc_personal;
CREATE TABLE ip_bank_abc_personal
(
    id                          number(11),
    logId                       number(11),
    collectionMethod            varchar2(100),
    IsCanBuy                    varchar2(1000),
    yzms                        varchar2(1000),
    sylx                        varchar2(1000),
    zdcyqx                      varchar2(1000),
    yjbjjz                      varchar2(1000),
    qgje                        varchar2(1000),
    xsqy                        varchar2(1000),
    xsqx                        varchar2(1000),
    PrdYildTypeOrder            varchar2(1000),
    cpbm                        varchar2(1000),
    cpmc                        varchar2(1000),
    fxjg                        varchar2(1000),
    yjjzIntro                   varchar2(1000),
    szComDat                    varchar2(1000),
    createTime                  varchar2(100)
);
-- 创建日志表的序列（用于主键自增）
--drop sequence seq_abc;
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
