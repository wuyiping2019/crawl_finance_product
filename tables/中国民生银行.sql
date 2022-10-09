--建表语句
drop table ip_bank_zgmsyh_personal;
create table ip_bank_zgmsyh_personal
(
    id         number(11),
    logId      number(11),
    cpmc       varchar2(1000), --产品名称
    cpmcjc     varchar2(1000), --产品名称简称
    cpbm       varchar2(1000), --产品编码
    cply       varchar2(1000), --产品来源
    hblx       varchar2(1000), --货币类型
    qgje       varchar2(1000), --起购金额
    fxdj       varchar2(1000), --风险等级
    cpqx       varchar2(1000), --产品期限
    ipo_kssj   varchar2(1000), --ipo开始时间
    ipo_jssj   varchar2(1000),--ipo结束时间
    cped       varchar2(1000),--产品总额度
    yjbjjz     varchar2(1000),--业绩比较基准
    zsjz       varchar2(1000),--指数基准
    cplx       varchar2(1000),--产品类型
    syms       varchar2(1000),--收益描述
    xygkfr     varchar2(1000),--下一个开放日
    ksrq       varchar2(1000),--开始日期
    jsrq       varchar2(1000),--结束日期
    cpkyed     varchar2(1000), --产品可用额度
    createTime varchar2(1000)
);
--创建sequence
create sequence seq_zgmsyh
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
--创建trigger
create or replace trigger trigger_zgmsyh
    before insert
    on ip_bank_zgmsyh_personal
    for each row
begin
    select seq_zgmsyh.nextval into :new.id from dual;
end;

truncate table ip_bank_zgmsyh_personal;
select count(1)
from ip_bank_zgmsyh_personal;
select *
from ip_bank_zgmsyh_personal order by createTime desc ;
where logId = 260
  and dwjz is not null
order by createTime desc;
select *
from SPIDER_LOG
order by id desc;

select *
from SPIDER_LOG
order by id desc;
select count(1) from ip_bank_zgmsyh_personal a where a.logId = 391;
show tables;

