-- 创建日志表
drop table ip_zglcw_bank;
create table ip_zglcw_bank
(
    id                  number(11, 0),
    logId               number(11, 0),
    djbm                varchar2(1000),
    fxjg                varchar2(1000),
    yzms                varchar2(1000),
    mjfs                varchar2(1000),
    qxlx                varchar2(1000),
    mjbz                varchar2(1000),
    tzxz                varchar2(1000),
    fxdj                varchar2(1000),
    mjqsrq              varchar2(1000),
    mjjsrq              varchar2(1000),
    cpqsrq              varchar2(1000),
    cpjsrq              varchar2(1000),
    ywqsrq              varchar2(1000),
    ywjsrq              varchar2(1000),
    sjts                varchar2(1000),
    csjz                varchar2(1000),
    cpjz                varchar2(1000),
    ljjz                varchar2(1000),
    zjycdfsyl           varchar2(1000),
    cptssx              varchar2(1000),
    tzzclx              varchar2(1000),
    dxjg                varchar2(1000),
    yqzdsyl             varchar2(1000),
    yqzgsyl             varchar2(1000),
    yjbjjzxx            varchar2(1000),
    yjbjjzsx            varchar2(1000),
    xsqy                varchar2(1000),
    createTime          varchar2(100)
);
-- 创建日志表的序列（用于主键自增）
drop sequence ip_zglcw_bank_id;
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