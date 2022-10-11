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

insert into ip_bank_zxyh_origin(NAV,endDate,realEndDate,nextEndDate,benchMarkMin,preIncomeRate,incomeRate,managerNo,controFlagS,sortRate,hasLimit,prdType,transCode,prdTypeName,workDateList,prdName,incomeType,nextIncomeRate,benchMarkBasis,ipoStartDate,prdAttrReal,channels,renegeInterTypeName,prdNextDate,limitFlag,isOnlyCust,usableAmt,btaType,prdTrustee,startDate,status,firstAmt,pfirstAmt,isNewCustPrd,investItem,riskLevel,fundMode,bmType,currType,remark,prdCode,prdGrpName,divModesName,prdGrpCode,contractFile,numOfIncomeRate,prdAttr,controlFlag,prdManagerName,instFlag,closeTime,liveTime,currTypeName,openTime,navDate,isDirectSell,topFlag,prdShortName,totAmt,prdStatus,isFinFund,ipoEndDate,livTimeUnitName,clientGroups,riskLevelName,benchMarkMax,branchNo,span,logId,createTime) values('1.0','2099-12-31','2099-12-31','','0.0','0.0','0.0205','MS','0','2.55','0','3','100200','收益型','[''2022-08-30'',''2022-08-31'',''2022-09-01'',''2022-09-02'']','非凡资产管理收益递增理财产品（个人款）','1','0.0255','','2021-11-15','1','01789b','','2022-09-01','1','0','0.0','','XXX','2021-11-16','0','50000.0','50000.0','0','    MS                                                      ','2','2','','156','None','FSAD21988A','','','','C1030521000492','2','A','101100011100010 00 00 0000   4000 0017110000000040','民生理财','0','133000','1','人民币','090000','20211116','0','2147483647','收益递增（个人款）','23860500000.0','0','False','2021-11-15','1天','12345P','较低风险(二级)','0.0','004','0','827','2022-08-31 10:45:57')
update spider_log set status='完成' , endDate='2022-08-31 10:45:57' , count='0' , result='失败' , detail='ORA-00942: table or view does not exist' , updateTime='2022-08-31 10:45:57'  where id = 827


