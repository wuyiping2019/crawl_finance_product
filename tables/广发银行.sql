--建表语句
drop table ip_bank_gfyh_personal;
create table ip_bank_gfyh_personal
(
    id         number(11),
    logId      number(11),
    cplx       varchar2(1000), --产品类型
    cpmc       varchar2(1000), --产品名称
    cpbm       varchar2(1000), --产品编码
    fxjg       varchar2(1000), --发行机构
    cply       varchar2(1000), --产品来源
    fxdj       varchar2(1000), --风险等级
    szkh       varchar2(1000), --受众客户
    qgje       varchar2(1000), --起购金额
    sfbz       varchar2(1000), --收费标准
    sffs       varchar2(1000),--收费方式
    cpqx       varchar2(1000),--产品期限
    createTime varchar2(1000)
);
--创建sequence
create sequence seq_gfyh
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
--创建trigger
create or replace trigger trigger_gfyh
    before insert
    on ip_bank_gfyh_personal
    for each row
begin
    select seq_gfyh.nextval into :new.id from dual;
end;

truncate table ip_bank_gfyh_personal;
select count(1)
from ip_bank_gfyh_personal;
select *
from ip_bank_gfyh_personal
where logId = 260
  and dwjz is not null
order by createTime desc;
select *
from SPIDER_LOG
order by id desc;

select *
from SPIDER_LOG
order by id desc;

