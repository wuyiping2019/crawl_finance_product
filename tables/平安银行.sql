--建表语句
drop table ip_bank_payh_personal;
create table ip_bank_payh_personal
(
    id         number(11),
    logId      number(11),
    cpbm       varchar2(1000),
    tadm varchar2(1000),
    cpmc       varchar2(1000),
    fxr       varchar2(1000),
    fxdj       varchar2(1000),
    qgje       varchar2(1000),
    xsqy       varchar2(1000),
    cpzt       varchar2(1000),
    cplx       varchar2(1000),
    djbh   varchar2(1000),
    cpfl       varchar2(1000),
    cply       varchar2(1000),
    cpgs varchar2(1000),
    createTime varchar2(1000)
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
from ip_bank_payh_personal order by createTime desc;
select *
from SPIDER_LOG
order by id desc;

select *
from SPIDER_LOG
order by id desc;

