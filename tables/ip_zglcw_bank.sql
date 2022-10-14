-- 创建日志表
--drop table ip_zglcw_bank;
create table ip_zglcw_bank
(
    id         number(11, 0),
    logId      number(11, 0),
    dm         varchar2(100),
    ms         varchar2(100),
    createTime varchar2(100)
);
-- 创建日志表的序列（用于主键自增）
create sequence ip_zglcw_bank_id
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;

create or replace trigger ip_zglcw_bank_trigger_id
    before insert
    on ip_zglcw_bank
    for each row
begin
    select ip_zglcw_bank_id.nextval into :new.id from dual;
end;
selECT * from ip_zglcw_bank
-- 测试
-- insert into ip_zglcw_bank(dm) values ('ceshi');
-- select * from ip_zglcw_bank;

-- select * from ip_zglcw_bank;
-- truncate table ip_zglcw_bank;