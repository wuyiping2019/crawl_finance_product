-- 创建招商银行个人理财产品表
drop table ip_bank_spdb_personal;
CREATE TABLE ip_bank_spdb_personal
(
    id                       number(11),
    logId                    number(11),
    finance_state            varchar2(1000),
    product_attr             varchar2(1000),
    finance_anticipate_rate  varchar2(1000),
    finance_limittime        varchar2(1000),
    finance_type             varchar2(1000),
    finance_lmttime_info     varchar2(1000),
    finance_allname          varchar2(1000),
    finance_indi_applminamnt varchar2(1000),
    docpuburl                varchar2(1000),
    finance_indi_ipominamnt  varchar2(1000),
    finance_next_openday     varchar2(1000),
    finance_ipoapp_flag      varchar2(1000),
    finance_no               varchar2(1000),
    finance_ipo_enddate      varchar2(1000),
    finance_risklevel        varchar2(1000),
    type                     varchar2(1000),
    createTime               varchar2(100)
);
-- 创建日志表的序列（用于主键自增）
--drop sequence seq_spdb;
create
    sequence seq_spdb
    minvalue 1
    maxvalue 99999999
    start
        with 1
    increment by 1
    cache
        50;
--创建触发器
create or
    replace trigger trigger_spdb
    before
        insert
    on ip_bank_spdb_personal
    for each row
begin
    select seq_spdb.nextval
    into :new.id
    from dual;
end;

truncate table ip_bank_spdb_personal;


insert into ip_bank_spdb_personal(docpuburl)
values ('ceshi');
select *
from ip_bank_spdb_personal;
