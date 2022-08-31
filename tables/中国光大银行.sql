--建表语句
drop table ip_bank_ceb_personal;
create table ip_bank_ceb_personal(
    id number(11),
    logId number(11),
    prdName varchar2(1000),
    prdCode varchar2(1000),
    prdProp varchar2(1000),
    risk varchar2(1000),
    detailType varchar2(1000),
    issuer varchar2(1000),
    onSale varchar2(1000),
    openCloseDuration varchar2(1000),
    prdDuration varchar2(1000),
    openRule varchar2(1000),
    minAmount varchar2(1000),
    saleRegion varchar2(1000),
    type varchar2(1000),
    createTime varchar2(1000)
);
--创建sequence
create sequence seq_ceb
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
--创建trigger
create or replace trigger trigger_ceb
    before insert
    on ip_bank_ceb_personal
    for each row
begin
    select seq_ceb.nextval into :new.id from dual;
end;

select * from ip_bank_ceb_personal order by createTime desc ;
select count(1) from ip_bank_ceb_personal a where a.logId = 493;
select * from SPIDER_LOG order by id desc;

