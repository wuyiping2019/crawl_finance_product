--建表语句
drop table ip_bank_cncb_personal;
create table ip_bank_cncb_personal(
    id number(11),
    logId number(11),
    prdCode varchar2(1000),
    prdName varchar2(1000),
    prdType varchar2(1000),
    nav varchar2(1000),
    prdStt varchar2(1000),
    riskLevel varchar2(1000),
    cpfl varchar2(1000),
    taCode varchar2(1000),
    taName varchar2(1000),
    prdAttr varchar2(1000),
    prdSttNm varchar2(1000),
    type varchar2(1000),
    createTime varchar2(1000)
);
--创建sequence
create sequence seq_cncb
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
--创建trigger
create or replace trigger trigger_cncb
    before insert
    on ip_bank_cncb_personal
    for each row
begin
    select seq_cncb.nextval into :new.id from dual;
end;

select * from ip_bank_cncb_personal;

