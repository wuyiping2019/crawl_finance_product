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
select * from spider_log;
truncate table spider_log;
insert into spider_log(name,startDate,status,createTime) values('中国理财网站','2022-08-17 13:07:02','正在执行中','2022-08-17 13:07:06');