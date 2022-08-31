-- 创建日志表
drop table spider_log;
create table spider_log
(
    id         number(11, 0),
    name       varchar2(100),
    status     varchar2(100),
    startDate  varchar2(100),
    endDate    varchar2(100),
    count      varchar2(100),
    result     varchar2(100),
    detail     varchar2(100),
    updateTime varchar2(100),
    createTime varchar2(100)
);
-- 创建日志表的序列（用于主键自增）
create sequence spider_log_id
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
drop trigger spider_log_trigger_id;
create or replace trigger spider_log_trigger_id
    before insert
    on spider_log
    for each row
begin
    select spider_log_id.nextval into :new.id from dual;
end;
/*
测试
insert into spider_log(name) values ('ceshi');
select * from spider_log;
*/
truncate table spider_log;
select * from spider_log order by id desc;
select * from IP_BANK_SPDB_PERSONAL a where a.LOGID = 34;
truncate table IP_BANK_SPDB_PERSONAL;
truncate table spider_log;
insert into spider_log(name,startDate,status,createTime) values('中国理财网站','2022-08-17 13:07:02','正在执行中','2022-08-17 13:07:06');
alter table spider_log modify (detail varchar2(3000));


drop table spider_log_detail;
create table spider_log_detail(
    id number(11),
    logId number(11),
    error_msg clob,
    create_time date default sysdate
);

-- 创建日志表的序列（用于主键自增）
create sequence spider_log_detail_id
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
create or replace trigger spider_log_detail_trigger_id
    before insert
    on spider_log_detail
    for each row
begin
    select spider_log_detail.nextval into :new.id from dual;
end;

select * from spider_log_detail;
select * from spider_log order by id desc ;
