-- 创建中国建设银行理财产品表
drop table ip_bank_ccb_personal;
CREATE TABLE ip_bank_ccb_personal
(
    id                                  number(11),
    logId                               number(11),
    BsPD_Cd                             varchar2(1000),
    IvsmPd_ECD                          varchar2(1000),
    Fnd_Nm                              varchar2(1000),
    ASPD_ID                             varchar2(1000),
    PD_Brnd_Nm                          varchar2(1000),
    Ivs_StDt                            varchar2(1000),
    Rlz_Cnd_ID                          varchar2(1000),
    Ec_Flag                             varchar2(1000),
    Ivs_CODt                            varchar2(1000),
    Scrtz_Cfm_Dt                        varchar2(1000),
    Scrtz_Udo_Dt                        varchar2(1000),
    Ivs_Trm                             varchar2(1000),
    LCS_Cd                              varchar2(1000),
    ChnBnd_Bsn_Trm                      varchar2(1000),
    Pft_Pcsg_Mod                        varchar2(1000),
    Exg_Pft_Cmnt                        varchar2(1000),
    Rs_StDt                             varchar2(1000),
    Rs_EdDt                             varchar2(1000),
    Prd_Num                             varchar2(1000),
    Othr_Ntw_Adr                        varchar2(1000),
    Web_Acs_Rsc_URL                     varchar2(1000),
    PD_Intd_Webst                       varchar2(1000),
    Cur_URL_Adr                         varchar2(1000),
    Src_URL_Adr                         varchar2(1000),
    CshEx_Cd                            varchar2(1000),
    HQ_Cfm_PdOrd_Ind                    varchar2(1000),
    PdOrd_Ddln_Dsc                      varchar2(1000),
    Rsk_Grd_Cd                          varchar2(1000),
    StTm                                varchar2(1000),
    EdTm                                varchar2(1000),
    PfCmpBss                            varchar2(1000),
    CcyCd                               varchar2(1000),
    Unit_Ast_NetVal                     varchar2(1000),
    NetVal_Dt                           varchar2(1000),
    SplLmt                              varchar2(1000),
    BnNm_Sz_UpLm_Val                    varchar2(1000),
    Stm_Src_Dsc                         varchar2(1000),
    Fnd_Clsd_Opn_TpCd                   varchar2(1000),
    Prblm_Dnum                          varchar2(1000),
    Rsrv_3                              varchar2(1000),
    Ins_Cvr_Inf_Dsc                     varchar2(1000),
    PgFc_Links_Adr                      varchar2(1000),
    Url                                 varchar2(1000),
    BsnItm_Dsc                          varchar2(1000),
    Exrc_Rule_Dsc                       varchar2(1000),
    Rmrk_1                              varchar2(1000),
    Rmrk_2                              varchar2(1000),
    Rmrk_3                              varchar2(1000),
    Land_Ntw_Adr                        varchar2(1000),
    Vd_Adr                              varchar2(1000),
    Grpg_Ind                            varchar2(1000),
    PD_Sl_Obj_Cd                        varchar2(1000),
    Txn_Mkt_ID                          varchar2(1000),
    Mix_PD_And_Brnd_Dsc                 varchar2(1000),
    Bkstg_PD_Tp_ECD                     varchar2(1000),
    Pcs_Mode                            varchar2(1000),
    IntAr_CODt                          varchar2(1000),
    PROM_TIME_A                         varchar2(1000),
    PROM_TIME_B                         varchar2(1000),
    PD_Tp_ID                            varchar2(1000),
    Cfm_Dys                             varchar2(1000),
    Wthr_Exst_Ind                       varchar2(1000),
    Co_Nm                               varchar2(1000),
    Rsc_URL                             varchar2(1000),
    Elc_CrdShp_URL_Adr                  varchar2(1000),
    ChrgFee_Cyc_StDt                    varchar2(1000),
    ChrgFee_Cyc_EdDt                    varchar2(1000),
    PD_Cnd                              varchar2(1000),
    Eclsv_Stm_ECD                       varchar2(1000),
    Txn_Cyc_s_Val                       varchar2(1000),
    Inpt_SrtDt                          varchar2(1000),
    Inpt_CODt                           varchar2(1000),
    IvsChmtPdPfCmpBss_Dsc               varchar2(1000),
    Ivs_ChmtPd_TpDs                     varchar2(1000),
    Stk_TpCd                            varchar2(1000),
    CorpPrvt_Cd                         varchar2(1000),
    Per_Txn_UprLmtAmt                   varchar2(1000),
    Per_Txn_LwrLmtAmt                   varchar2(1000),
    Txn_Tp_LrgClss_Cd                   varchar2(1000),
    Fnd_Idv_Book_Hgst_Lot               varchar2(1000),
    SpLn_Val                            varchar2(1000),
    PD_Fcn_Cd                           varchar2(1000),
    Ntc_Msg_Ttl_Inf                     varchar2(1000),
    Rsrv_1                              varchar2(1000),
    Br_Rmrk_1                           varchar2(1000),
    Rcmm_Txn_TpCd                       varchar2(1000),
    Txn_BO_ID                           varchar2(1000),
    Rsrv_2                              varchar2(1000),
    Spvs_Prvn_Dsc                       varchar2(1000),
    Chk_Img_Opn_Ind                     varchar2(1000),
    createTime                          varchar2(100)
);
-- 创建日志表的序列（用于主键自增）
--drop sequence seq_ccb;
create sequence seq_ccb
    minvalue 1
    maxvalue 99999999
    start with 1
    increment by 1
    cache 50;
--创建触发器
create or replace trigger trigger_ccb
    before insert
    on ip_bank_ccb_personal
    for each row
begin
    select seq_ccb.nextval into :new.id from dual;
end;

truncate table ip_bank_ccb_personal;


insert into ip_bank_ccb_personal(PrdName) values ('ceshi');
select * from ip_bank_ccb_personal;
