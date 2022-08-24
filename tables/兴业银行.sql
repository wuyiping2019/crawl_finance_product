--建表语句
drop table ip_bank_xyyh_personal;
create table ip_bank_xyyh_personal
(
    id         number(11),
    logId      number(11),
    cpmc       varchar2(1000), --产品名称
    cpbm       varchar2(1000), --产品编码
    cply       varchar2(1000), --产品来源
    hblx       varchar2(1000), --货币类型
    qgje       varchar2(1000), --起购金额
    fxdj       varchar2(1000), --风险等级
    xsqy       varchar2(1000), --销售区域
    cpqx       varchar2(1000), --产品期限
    ipo_kssj   varchar2(1000), --ipo开始时间
    ipo_jssj   varchar2(1000),--ipo结束时间
    yjbjjz     varchar2(1000),--业绩比较基准
    cplx       varchar2(1000),--产品类型
    fxr        varchar2(1000),--发行人
    createTime varchar2(1000)
);
--创建sequence
create sequence seq_xyyh
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
--创建trigger
create or replace trigger trigger_xyyh
    before insert
    on ip_bank_xyyh_personal
    for each row
begin
    select seq_xyyh.nextval into :new.id from dual;
end;

truncate table ip_bank_xyyh_personal;
select count(1)
from ip_bank_xyyh_personal;
select *
from ip_bank_xyyh_personal;
select *
from SPIDER_LOG
order by id desc;

select *
from SPIDER_LOG
order by id desc;

