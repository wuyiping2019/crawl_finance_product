--建表语句
drop table ip_bank_hxyh_personal;
create table ip_bank_hxyh_personal
(
    id         number(11),
    logId      number(11),
    innerId    varchar2(1000),
    prdName    varchar2(1000), --理财名称
    prdCode    varchar2(1000), --理财编码
    dwjz       varchar2(1000), --单位净值
    yqnhsyl    varchar2(1000), --预期年化收益率
    yjjz       varchar2(1000), --业绩基准
    qrnhsyl    varchar2(1000), --7日年化收益率
    yqzgnhsyl  varchar2(1000), --预期最高年化收益率
    qx         varchar2(1000), --期限（无固定期限\10天\1年）
    fsrq       varchar2(1000),--发售日期
    qgje       varchar2(1000),--起购金额
    gmqd       varchar2(1000),--购买渠道
    fxjg       varchar2(1000),--发行机构
    lcsms      varchar2(1000),--理财说明书
    type       varchar2(1000),
    createTime varchar2(1000)
);
--创建sequence
create sequence seq_hxyh
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;

--创建trigger
create or replace trigger trigger_hxyh
    before insert
    on ip_bank_hxyh_personal
    for each row
begin
    select seq_hxyh.nextval into :new.id from dual;
end;

truncate table ip_bank_hxyh_personal;
select count(1)
from ip_bank_hxyh_personal;
select *
from ip_bank_hxyh_personal order by createTime desc;
select *
from SPIDER_LOG
order by id desc;
select count(1) from ip_bank_hxyh_personal a where a.logId = 262;
select *
from SPIDER_LOG
order by id desc;

