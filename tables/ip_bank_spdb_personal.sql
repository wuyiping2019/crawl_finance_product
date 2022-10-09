-- 创建招商银行个人理财产品表
drop table ip_bank_spdb_personal;
CREATE TABLE ip_bank_spdb_personal
(
    id                                      number(11),
    logId                                   number(11),
    cpzt                                    varchar2(1000),
    cpsms                                   varchar2(1000),
    yqnhsyl                                 varchar2(1000),
    finance_limittime                       varchar2(1000),
    finance_type                            varchar2(1000),
    tzqx                                    varchar2(1000),
    cpmc                                    varchar2(1000),
    scrgxx                                  varchar2(1000),
    docpuburl                               varchar2(1000),
    qgje                                    varchar2(1000),
    finance_next_openday                    varchar2(1000),
    finance_ipoapp_flag                     varchar2(1000),
    cpbm                                    varchar2(1000),
    mjjsrq                                  varchar2(1000),
    fxdj                                    varchar2(1000),
    yzms                                    varchar2(1000),
    dzje                                    varchar2(1000),
    fsqsrq                                  varchar2(1000),
    khsygz                                  varchar2(1000),
    FinanceAbbrName                         varchar2(1000),
    EVoucherCode                            varchar2(1000),
    RaiseType                               varchar2(1000),
    FinanceOwner                            varchar2(1000),
    RegionOpenFlag                          varchar2(1000),
    FinanceShowBenchmark                    varchar2(1000),
    FinanceProductLabelUrl                  varchar2(1000),
    FinanceMinHoldings                      varchar2(1000),
    PerformanceRewards                      varchar2(1000),
    FinanceReturnRateNameDesc               varchar2(1000),
    FinanceCurrency                         varchar2(1000),
    cpclr                                   varchar2(1000),
    FinanceFee                              varchar2(1000),
    FinanceShowMsgMark                      varchar2(1000),
    FinancePurchaseStartTime                varchar2(1000),
    FinanceEarningsFlag                     varchar2(1000),
    FinanceExpireDate                       varchar2(1000),
    FinanceKind                             varchar2(1000),
    CurrentDate                             varchar2(1000),
    HasHoldings                             varchar2(1000),
    RegistrantNo                            varchar2(1000),
    FinanceBenchmarkDesc                    varchar2(1000),
    fxjg                                    varchar2(1000),
    ChartTips                               varchar2(1000),
    EVoucherCodeMaxAmnt                     varchar2(1000),
    EndEntryRule                            varchar2(1000),
    InvestmentOrientation                   varchar2(1000),
    FinanceFastRedemptionRule               varchar2(1000),
    IsFavorite                              varchar2(1000),
    EVoucherCodeMinAmnt                     varchar2(1000),
    fsjzrq                                  varchar2(1000),
    FinanceRedemptionStartDate              varchar2(1000),
    FinanceReturnRateName                   varchar2(1000),
    FinanceBrand                            varchar2(1000),
    FinanceTransStage                       varchar2(1000),
    FinanceBenchmark                        varchar2(1000),
    EndRule                                 varchar2(1000),
    syqsrq                                  varchar2(1000),
    RiskAlert                               varchar2(1000),
    ProductSegmentFlag                      varchar2(1000),
    FinanceReturnRateLevel                  varchar2(1000),
    FinanceShareLevel                       varchar2(1000),
    FinanceEarningsType                     varchar2(1000),
    FinanceShowYieldEstimation              varchar2(1000),
    FinanceRedemptionFee                    varchar2(1000),
    DigitalHuman                            varchar2(1000),
    FinancePurchaserNum                     varchar2(1000),
    FinanceReturnRate                       varchar2(1000),
    AssetMgmtIntro                          varchar2(1000),
    FinanceType                             varchar2(1000),
    PerformanceRewardsDesc                  varchar2(1000),
    shgz                                    varchar2(1000),
    FinanceFastRedemptionMaxAmnt            varchar2(1000),
    FinanceValidity                         varchar2(1000),
    dzgz                                    varchar2(1000),
    qxgz                                    varchar2(1000),
    FinanceMinRedemption                    varchar2(1000),
    DefaultTab                              varchar2(1000),
    MovementsType                           varchar2(1000),
    FinanceExpectedYield                    varchar2(1000),
    createTime                              varchar2(100)
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
select * from ip_bank_spdb_personal order by createTime desc ;
truncate table ip_bank_spdb_personal;
select count(1) from ip_bank_spdb_personal a where a.logId = 57
