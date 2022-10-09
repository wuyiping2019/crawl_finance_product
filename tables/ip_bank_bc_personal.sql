-- 创建中国银行理财产品表
drop table ip_bank_bc_personal;
CREATE TABLE ip_bank_bc_personal
(
    id             number(11),
    logId          number(11),
    cpbm           varchar2(100),
    cpmc           varchar2(1000),
    cpqx           varchar2(1000),
    yjbjjz_jz      varchar2(1000),
    qgje           varchar2(1000),
    xsqd           varchar2(1000),
    fxdj           varchar2(1000),
    mjqsrq         varchar2(1000),
    mjjsrq         varchar2(1000),
    qxr            varchar2(1000),
    fbq            varchar2(1000),
    tzzlx          varchar2(1000),
    xsqy           varchar2(1000),
    sggzsm         varchar2(1000),
    hsgzsm         varchar2(1000),
    cplx           varchar2(1000),
    createTime     varchar2(100)
);
-- 创建日志表的序列（用于主键自增）
drop sequence seq_bc;
create sequence seq_bc
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
--创建触发器
create or replace trigger trigger_bc
    before insert
    on ip_bank_bc_personal
    for each row
begin
    select seq_bc.nextval into :new.id from dual;
end;

truncate table ip_bank_bc_personal;

