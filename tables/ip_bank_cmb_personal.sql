-- 创建招商银行个人理财产品表
drop table ip_bank_cmb_personal;
CREATE TABLE ip_bank_cmb_personal
(
    id                 number(11),
    logId              number(11),
    innerId            number(11),
    PrdCode            varchar2(100),
    PrdName            varchar2(1000),
    PrdBrief           varchar2(1000),
    TypeCode           varchar2(1000),
    AreaCode           varchar2(1000),
    Currency           varchar2(1000),
    BeginDate          varchar2(1000),
    EndDate            varchar2(1000),
    ShowExpireDate     varchar2(1000),
    ExpireDate         varchar2(1000),
    Status             varchar2(1000),
    NetValue           varchar2(1000),
    IsNewFlag          varchar2(1000),
    Term               varchar2(1000),
    Style              varchar2(1000),
    InitMoney          varchar2(1000),
    IncresingMoney     varchar2(1000),
    Risk               varchar2(1000),
    FinDate            varchar2(1000),
    SaleChannel        varchar2(1000),
    SaleChannelName    varchar2(1000),
    IsCanBuy           varchar2(1000),
    REGCode            varchar2(1000),
    CapitalProtectName varchar2(1000),
    RateFlag           varchar2(1000),
    CRateType          varchar2(1000),
    RateLow            varchar2(1000),
    RateHigh           varchar2(1000),
    IsInfinite         varchar2(1000),
    ShowExpectedReturn varchar2(1000),
    ProxyText          varchar2(1000),
    IsSA               varchar2(1000),
    saaCod             varchar2(1000),
    funCod             varchar2(1000),
    datCyl             varchar2(1000),
    invUntName         varchar2(1000),
    createTime         varchar2(100)
);
-- 创建日志表的序列（用于主键自增）
drop sequence seq_cmb;
create sequence seq_cmb
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
--创建触发器
create or replace trigger trigger_cmb
    before insert
    on ip_bank_cmb_personal
    for each row
begin
    select seq_cmb.nextval into :new.id from dual;
end;

truncate table ip_bank_cmb_personal;


insert into ip_bank_cmb_personal(PrdName) values ('ceshi');
select * from ip_bank_cmb_personal;
