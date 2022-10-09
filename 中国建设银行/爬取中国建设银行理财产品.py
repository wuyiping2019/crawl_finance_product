import json
import math

from requests import Session

from utils.mark_log import insertLogToDB
from utils.spider_flow import process_flow

TAGET_TABLE = 'ip_bank_ccb_personal'
LOG_NAME = '中国建设银行'
prodCode = []


Rsk_Grd= {
    "01": "R1风险极低",
    "02": "R2较低风险",
    "03": "R3中等风险",
    "04": "R4较高风险",
    "05": "R5高风险"
}

City= {
    "340000000": "安徽省","110000000": "北京市","500000000": "重庆市","212000000": "大连市","350000000": "福建省","440000000": "广东省","450000000": "广西省","620000000": "甘肃省",
    "520000000": "贵州省","460000000": "海南省","130000000": "河北省","410000000": "河南省","230000000": "黑龙江","420000000": "湖北省","430000000": "湖南省","220000000": "吉林省",
    "320000000": "江苏省","360000000": "江西省","210000000": "辽宁省","150000000": "内蒙古","331000000": "宁波市","640000000": "宁夏区","371000000": "青岛市","630000000": "青海省",
    "370000000": "山东省","140000000": "山西省","610000000": "陕西省","310000000": "上海市","510000000": "四川省","442000000": "深圳市","322000000": "苏州市","120000000": "天津市",
    "351000000": "厦门市","540000000": "西藏区","650000000": "新疆区","530000000": "云南省","330000000": "浙江省"
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'Referer': 'http://finance3.ccb.com/cn/finance/products/self/product_list.html',
    'Host': 'finance3.ccb.com',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
}

TYPES = [
    {
        'url': 'http://finance3.ccb.com/tran/WCCMainPlatV5',
        'method': 'get',
        'requestParams': {
            "CCB_IBSVersion": "V5",
            "SERVLET_NAME": "WCCMainPlatV5",
            "TXCODE": "NLCQ11",
            "Fcn_Cd": "0",
            "REC_IN_PAGE": "10",
            "PAGE_JUMP": "1",
            "Sel_StCd": "8",
            "Txn_BO_ID": "340000000",
            "Chnl_ID": "10060009",
            "FndCo_Agnc_Sale_InsID": "005",
            "Crt_Chnl_ID": "9999999999",
            "PD_Sl_Obj_Cd": "01",
            "Bkstg_PD_Tp_ECD": "02"
        }
    },
    {
        'url': 'http://finance3.ccb.com/tran/WCCMainPlatV5',
        'method': 'get',
        'requestParams': {
            "CCB_IBSVersion": "V5",
            "SERVLET_NAME": "WCCMainPlatV5",
            "TXCODE": "NLCQ11",
            "Fcn_Cd": "0",
            "REC_IN_PAGE": "10",
            "PAGE_JUMP": "1",
            "Sel_StCd": "9",
            "Txn_BO_ID": "110000000",
            "Chnl_ID": "10060009",
            "FndCo_Agnc_Sale_InsID": "005",
            "Crt_Chnl_ID": "9999999999",
            "PD_Sl_Obj_Cd": "01",
            "Bkstg_PD_Tp_ECD": "01"
        }
    }
]


def process_spider_detail(conn, cursor, session: Session, log_id: int, **kwargs):
    """
    处理爬虫细节的回调函数
    :param conn:自动传参
    :param cursor: 自动传参
    :param session: 自动传参
    :param log_id: 自动传参
    :param kwargs: 额外需要的参数 手动传入
    :return:
    """
    for type in TYPES:
        request_url = type['url']
        request_method = type['method']
        request_params = type['requestParams']
        for k, v in City.items():
            request_params['Txn_BO_ID'] = k
            print(request_params['Txn_BO_ID'])
            resp = session.request(method=request_method, url=request_url, params=request_params, headers=HEADERS).text
            # 获取总页数
            total_page = int(json.loads(resp)['TOTAL_PAGE'])
            # 循环处理每页数据
            loop_pages(total_page, session, cursor, log_id, request_url, request_method, request_params)


def process_one_page(session: Session,
                     cur,
                     log_id: int,
                     request_url: str,
                     request_method: str,
                     request_params: dict):
    """
    处理爬取的一页数据并写入数据库
    :param session:
    :param cur:
    :param log_id: 日志id
    :param page: 处理的页数
    :param request_url: 请求url
    :param request_method: 请求方法
    :param request_params: 请求参数
    :return:
    """
    resp = session.request(method=request_method, url=request_url, params=request_params, headers=HEADERS).text
    rows = parse_table(resp,request_params)
    for row in rows:
        row['logId'] = log_id
        insertLogToDB(cur, row, TAGET_TABLE)


def loop_pages(total_pages: int,
               session: Session,
               cur,
               log_id: int,
               request_url: str,
               request_method: str,
               request_params: dict):
    """
    按总页数 循环读取各页数据并处理每一页的数据
    :param cur:
    :param session:
    :param log_id:
    :param request_params:
    :param request_method:
    :param request_url:
    :param total_pages:
    :return:
    """
    for page in range(1, total_pages + 1, 1):
        request_params['PAGE_JUMP'] = str(page)
        process_one_page(session, cur, log_id, request_url, request_method, request_params)



def parse_table(table_str,request_params):
    """
    解析页面的产品 返回一个产品列表
    :param table_str:
    :return:
    """
    if table_str in table_str:
        reps = json.loads(table_str)['PROD_INFO_GRP']
        # tables = rep['Table']
        rows = []
        for product in reps:
            global prodCode
            if not prodCode.__len__() == 0:
                if product['IvsmPd_ECD'] in prodCode:
                    continue
            Rsk_Grd_Cd=product['Rsk_Grd_Cd']
            Rsk_Grd_Name = Rsk_Grd[Rsk_Grd_Cd]

            Prod_Sel_GRP = product['Prod_Sel_GRP']
            Txn_Num_GRP = product['Txn_Num_GRP']
            Prod_Func_GRP = product['Prod_Func_GRP']
            Prod_Hig_Features_GPR = product['Prod_Hig_Features_GPR']
            Rcmm_Txn = product['Rcmm_Txn']
            Sale_Inst_ECD_GRP = product['Sale_Inst_ECD_GRP']
            om_Problems = product['om_Problems']
            Auto_Invest_Plan_GPR = product['Auto_Invest_Plan_GPR']
            Stk_TpCdList=[]
            CorpPrvt_CdList=[]
            Per_Txn_UprLmtAmtList=[]
            Per_Txn_LwrLmtAmtList=[]
            Txn_Tp_LrgClss_CdList=[]
            Fnd_Idv_Book_Hgst_LotList=[]
            SpLn_ValList=[]
            PD_Fcn_CdList=[]
            Ntc_Msg_Ttl_InfList=[]
            Rsrv_1List=[]
            Br_Rmrk_1List=[]
            Rcmm_Txn_TpCdList=[]
            Txn_BO_IDList=[]
            Rsrv_2List=[]
            Spvs_Prvn_DscList=[]
            Chk_Img_Opn_IndList=[]
            for Stk_TpCd in Prod_Sel_GRP:
                if not Stk_TpCd['Stk_TpCd'] == '':
                    Stk_TpCdList.append(Stk_TpCd['Stk_TpCd'])
            Stk_TpCds = ','.join(Stk_TpCdList)

            for CorpPrvt_Cd in Txn_Num_GRP:
                if not CorpPrvt_Cd['CorpPrvt_Cd'] == '':
                    CorpPrvt_CdList.append(CorpPrvt_Cd['CorpPrvt_Cd'])
            CorpPrvt_Cd = ','.join(CorpPrvt_CdList)
            for Per_Txn_UprLmtAmt in Txn_Num_GRP:
                if not Per_Txn_UprLmtAmt['Per_Txn_UprLmtAmt'] == '':
                    Per_Txn_UprLmtAmtList.append(Per_Txn_UprLmtAmt['Per_Txn_UprLmtAmt'])
            Per_Txn_UprLmtAmt = ','.join(Per_Txn_UprLmtAmtList)
            for Per_Txn_LwrLmtAmt in Txn_Num_GRP:
                if not Per_Txn_LwrLmtAmt['Per_Txn_LwrLmtAmt'] == '':
                    Per_Txn_LwrLmtAmtList.append(Per_Txn_LwrLmtAmt['Per_Txn_LwrLmtAmt'])
            Per_Txn_LwrLmtAmt = ','.join(Per_Txn_LwrLmtAmtList)
            for Txn_Tp_LrgClss_Cd in Txn_Num_GRP:
                if not Txn_Tp_LrgClss_Cd['Txn_Tp_LrgClss_Cd'] == '':
                    Txn_Tp_LrgClss_CdList.append(Txn_Tp_LrgClss_Cd['Txn_Tp_LrgClss_Cd'])
            Txn_Tp_LrgClss_Cd = ','.join(Txn_Tp_LrgClss_CdList)
            for Fnd_Idv_Book_Hgst_Lot in Txn_Num_GRP:
                if not Fnd_Idv_Book_Hgst_Lot['Fnd_Idv_Book_Hgst_Lot'] == '':
                    Fnd_Idv_Book_Hgst_LotList.append(Fnd_Idv_Book_Hgst_Lot['Fnd_Idv_Book_Hgst_Lot'])
            Fnd_Idv_Book_Hgst_Lot = ','.join(Fnd_Idv_Book_Hgst_LotList)
            for SpLn_Val in Txn_Num_GRP:
                if not SpLn_Val['SpLn_Val'] == '':
                    SpLn_ValList.append(SpLn_Val['SpLn_Val'])
            SpLn_Val = ','.join(SpLn_ValList)

            for PD_Fcn_Cd in Prod_Func_GRP:
                if not PD_Fcn_Cd['PD_Fcn_Cd'] == '':
                    PD_Fcn_CdList.append(PD_Fcn_Cd['PD_Fcn_Cd'])
            PD_Fcn_Cd = ','.join(PD_Fcn_CdList)

            for Ntc_Msg_Ttl_Inf in Prod_Hig_Features_GPR:
                if not Ntc_Msg_Ttl_Inf['Ntc_Msg_Ttl_Inf'] == '':
                    Ntc_Msg_Ttl_InfList.append(Ntc_Msg_Ttl_Inf['Ntc_Msg_Ttl_Inf'])
            Ntc_Msg_Ttl_Inf = ','.join(Ntc_Msg_Ttl_InfList)
            for Rsrv_1 in Prod_Hig_Features_GPR:
                if not Rsrv_1['Rsrv_1'] == '':
                    Rsrv_1List.append(Rsrv_1['Rsrv_1'])
            Rsrv_1 = ','.join(Rsrv_1List)
            for Br_Rmrk_1 in Prod_Hig_Features_GPR:
                if not Br_Rmrk_1['Br_Rmrk_1'] == '':
                    Br_Rmrk_1List.append(Br_Rmrk_1['Br_Rmrk_1'])
            Br_Rmrk_1 = ','.join(Br_Rmrk_1List)

            for Rcmm_Txn_TpCd in Rcmm_Txn:
                if not Rcmm_Txn_TpCd['Rcmm_Txn_TpCd'] == '':
                    Rcmm_Txn_TpCdList.append(Rcmm_Txn_TpCd['Rcmm_Txn_TpCd'])
            Rcmm_Txn_TpCd = ','.join(Rcmm_Txn_TpCdList)

            for Txn_BO_ID in Sale_Inst_ECD_GRP:
                Txn_BO_ID=Txn_BO_ID['Txn_BO_ID']
                if Txn_BO_ID == '999999999':
                    Txn_BO_IDList.append('全国')
                else:
                    Txn_BO_IDList.append(City[Txn_BO_ID])
            Txn_BO_ID = ','.join(Txn_BO_IDList)

            for Rsrv_2 in om_Problems:
                if not Rsrv_2['Rsrv_2'] == '':
                    Rsrv_2List.append(Rsrv_2['Rsrv_2'])
            Rsrv_2 = ','.join(Rsrv_2List)
            for Spvs_Prvn_Dsc in om_Problems:
                if not Spvs_Prvn_Dsc['Spvs_Prvn_Dsc'] == '':
                    Spvs_Prvn_DscList.append(Spvs_Prvn_Dsc['Spvs_Prvn_Dsc'])
            Spvs_Prvn_Dsc = ','.join(Spvs_Prvn_DscList)

            for Chk_Img_Opn_Ind in Auto_Invest_Plan_GPR:
                if not Chk_Img_Opn_Ind['Chk_Img_Opn_Ind'] == '':
                    Chk_Img_Opn_IndList.append(Chk_Img_Opn_Ind['Chk_Img_Opn_Ind'])
            Chk_Img_Opn_Ind = ','.join(Chk_Img_Opn_IndList)
            row = {
                'BsPD_Cd': product['BsPD_Cd'],
                'cpbm': product['IvsmPd_ECD'],
                'cpmc': product['Fnd_Nm'],
                'ASPD_ID': product['ASPD_ID'],
                'PD_Brnd_Nm': product['PD_Brnd_Nm'],
                'fsqsrq': product['Ivs_StDt'],
                'Rlz_Cnd_ID': product['Rlz_Cnd_ID'],
                'Ec_Flag': product['Ec_Flag'],
                'fsjzrq': product['Ivs_CODt'],
                'Scrtz_Cfm_Dt': product['Scrtz_Cfm_Dt'],
                'Scrtz_Udo_Dt': product['Scrtz_Udo_Dt'],
                'cpxq': product['Ivs_Trm'],
                'ssgz': product['LCS_Cd'],
                'ChnBnd_Bsn_Trm': product['ChnBnd_Bsn_Trm'],
                'Pft_Pcsg_Mod': product['Pft_Pcsg_Mod'],
                'yjbjjz': product['Exg_Pft_Cmnt'],
                'mjqsrq': product['Rs_StDt'],
                'mjjsrq': product['Rs_EdDt'],
                'Prd_Num': product['Prd_Num'],
                'Othr_Ntw_Adr': product['Othr_Ntw_Adr'],
                'cpsms': product['Web_Acs_Rsc_URL'],
                'PD_Intd_Webst': product['PD_Intd_Webst'],
                'Cur_URL_Adr': product['Cur_URL_Adr'],
                'Src_URL_Adr': product['Src_URL_Adr'],
                'CshEx_Cd': product['CshEx_Cd'],
                'HQ_Cfm_PdOrd_Ind': product['HQ_Cfm_PdOrd_Ind'],
                'PdOrd_Ddln_Dsc': product['PdOrd_Ddln_Dsc'],
                'fxdj': Rsk_Grd_Name,
                'StTm': product['StTm'],
                'EdTm': product['EdTm'],
                'PfCmpBss': product['PfCmpBss'],
                'CcyCd': product['CcyCd'],
                'mwfsy': product['Unit_Ast_NetVal'],
                'NetVal_Dt': product['NetVal_Dt'],
                'SplLmt': product['SplLmt'],
                'BnNm_Sz_UpLm_Val': product['BnNm_Sz_UpLm_Val'],
                'Stm_Src_Dsc': product['Stm_Src_Dsc'],
                'Fnd_Clsd_Opn_TpCd': product['Fnd_Clsd_Opn_TpCd'],
                'Prblm_Dnum': product['Prblm_Dnum'],
                'Rsrv_3': product['Rsrv_3'],
                'Ins_Cvr_Inf_Dsc': product['Ins_Cvr_Inf_Dsc'],
                'PgFc_Links_Adr': product['PgFc_Links_Adr'],
                'Url': product['Url'],
                'BsnItm_Dsc': product['BsnItm_Dsc'],
                'Exrc_Rule_Dsc': product['Exrc_Rule_Dsc'],
                'sggz': product['Rmrk_1'],
                'khsygz': product['Rmrk_2'],
                'shgz': product['Rmrk_3'],
                'Land_Ntw_Adr': product['Land_Ntw_Adr'],
                'Vd_Adr': product['Vd_Adr'],
                'Grpg_Ind': product['Grpg_Ind'],
                'PD_Sl_Obj_Cd': product['PD_Sl_Obj_Cd'],
                'Txn_Mkt_ID': product['Txn_Mkt_ID'],
                'tzqx': product['Mix_PD_And_Brnd_Dsc'],
                'Bkstg_PD_Tp_ECD': product['Bkstg_PD_Tp_ECD'],
                'Pcs_Mode': product['Pcs_Mode'],
                'IntAr_CODt': product['IntAr_CODt'],
                'PROM_TIME_A': product['PROM_TIME_A'],
                'PROM_TIME_B': product['PROM_TIME_B'],
                'PD_Tp_ID': product['PD_Tp_ID'],
                'Cfm_Dys': product['Cfm_Dys'],
                'Wthr_Exst_Ind': product['Wthr_Exst_Ind'],
                'fxjg': product['Co_Nm'],
                'Rsc_URL': product['Rsc_URL'],
                'Elc_CrdShp_URL_Adr': product['Elc_CrdShp_URL_Adr'],
                'ChrgFee_Cyc_StDt': product['ChrgFee_Cyc_StDt'],
                'ChrgFee_Cyc_EdDt': product['ChrgFee_Cyc_EdDt'],
                'PD_Cnd': product['PD_Cnd'],
                'Eclsv_Stm_ECD': product['Eclsv_Stm_ECD'],
                'Txn_Cyc_s_Val': product['Txn_Cyc_s_Val'],
                'Inpt_SrtDt': product['Inpt_SrtDt'],
                'Inpt_CODt': product['Inpt_CODt'],
                'IvsChmtPdPfCmpBss_Dsc': product['IvsChmtPdPfCmpBss_Dsc'],
                'Ivs_ChmtPd_TpDs': product['Ivs_ChmtPd_TpDs'],
                'Stk_TpCd': Stk_TpCds,
                'CorpPrvt_Cd': CorpPrvt_Cd,
                'Per_Txn_UprLmtAmt': Per_Txn_UprLmtAmt,
                'qgje': Per_Txn_LwrLmtAmt,
                'Txn_Tp_LrgClss_Cd': Txn_Tp_LrgClss_Cd,
                'Fnd_Idv_Book_Hgst_Lot': Fnd_Idv_Book_Hgst_Lot,
                'SpLn_Val': SpLn_Val,
                'PD_Fcn_Cd': PD_Fcn_Cd,
                'Ntc_Msg_Ttl_Inf': Ntc_Msg_Ttl_Inf,
                'Rsrv_1': Rsrv_1,
                'Br_Rmrk_1': Br_Rmrk_1,
                'Rcmm_Txn_TpCd': Rcmm_Txn_TpCd,
                'xsqy': Txn_BO_ID,
                'cjwt': Rsrv_2,
                'wtda': Spvs_Prvn_Dsc,
                'Chk_Img_Opn_Ind': Chk_Img_Opn_Ind
            }
            prodCode.append(product['IvsmPd_ECD'])
            rows.append(row)
        return rows


if __name__ == '__main__':
    process_flow(log_name=LOG_NAME,
                 target_table=TAGET_TABLE,
                 callback=process_spider_detail
                 )
