--建表语句
drop table ip_bank_payh_personal;
create table ip_bank_payh_personal
(
    id         number(11),
    logId      number(11),
    cpmc       varchar2(1000), --产品名称
    cpbm       varchar2(1000), --产品编码
    gljg       varchar2(1000), --管理结构
    fxdj       varchar2(1000), --风险等级
    qgje       varchar2(1000), --起购金额
    xsqy       varchar2(1000), --销售区域
    cpzt       varchar2(1000), --产品状态
    cplx       varchar2(1000),--产品类型
    djbh       varchar2(1000),--登记编号
    sfbzjsffs  varchar2(1000),--收费标准及收费方式
    cpfl       varchar2(1000),--产品分类
    cply      varchar2(1000),--产品来源
    fxr        varchar2(1000),--发行人
    createTime varchar2(1000)
);
--创建sequence
create sequence seq_payh
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
--创建trigger
create or replace trigger trigger_payh
    before insert
    on ip_bank_payh_personal
    for each row
begin
    select seq_payh.nextval into :new.id from dual;
end;

truncate table ip_bank_payh_personal;
select count(1)
from ip_bank_payh_personal;
select *
from ip_bank_payh_personal;
select *
from SPIDER_LOG
order by id desc;

select *
from SPIDER_LOG
order by id desc;

