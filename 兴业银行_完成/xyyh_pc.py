import time

import requests
from bs4 import BeautifulSoup
from requests import Session

from crawl_utils.common_utils import delete_empty_value
from crawl_utils.custom_exception import cast_exception
from crawl_utils.html_utils import parse_table
from crawl_utils.mappings import FIELD_MAPPINGS
from xyyh_config import PC_BHLCCP_REQUEST, SLEEP_SECOND, PC_QTLCCP_DX_REQUEST, PC_LCCP_DX_REQUEST, PC_JJ_DX_REQUEST, \
    PC_QSJH_DX_REQUEST, PC_JGXCK_REQUEST, PC_XT_DX_REQUEST, PC_BX_DX_REQUEST


# https://www.cib.com.cn/cn/personal/wealth-management/xxcx/table/

def transform_row(rows, ):
    processed_rows = []
    for row in rows:
        print(row)
        mjfs_list = [row.get('募集方式', None), row.get('募集方式（公募/私募）', None)]
        mjfs = None
        for item in mjfs_list:
            if item:
                mjfs = item
                break
        fxdj_list = [row.get('代销风险等级', None), row.get('产品风险等级', None), row.get('产品风险级别', None)]
        fxdj = None
        for item in fxdj_list:
            if item:
                fxdj = item
                break
        cpbm_list = [row.get('产品代码', None), row.get('产品代号', None)]
        cpbm = None
        for item in cpbm_list:
            if item:
                cpbm = item
                break
        processed_row = {
            FIELD_MAPPINGS['募集方式']: mjfs,
            FIELD_MAPPINGS['产品名称']: row.get('产品名称', None),
            FIELD_MAPPINGS['登记编码']: row.get('银行业理财信息登记系统登记编码', None),
            FIELD_MAPPINGS['产品编码']: cpbm,
            FIELD_MAPPINGS['产品简称']: row.get('产品简称', None),
            FIELD_MAPPINGS['风险等级']: fxdj,
            FIELD_MAPPINGS['管理人']: row.get('发行机构', None),
            FIELD_MAPPINGS['投资性质']: row.get('产品类型', None),
            FIELD_MAPPINGS['起息日']: row.get('产品起息日', None),
            FIELD_MAPPINGS['预计到期利率']: row.get('预计到期利率（%）', None),
            FIELD_MAPPINGS['产品状态']: row.get('产品状态', None),

        }
        delete_empty_value(processed_row)
        print(processed_row)
        processed_row['mark'] = 'PC'
        processed_rows.append(processed_row)
    return processed_rows


def do_request(session: Session, method, url, headers, data):
    response = session.request(method=method, url=url, headers=headers, data=data)
    time.sleep(SLEEP_SECOND)
    return response.text.encode(response.encoding).decode('utf-8') if response.encoding else response.text


def do_parse(resp_str, col_names, head_tag, callbacks={}):
    try:
        soup = BeautifulSoup(resp_str, 'lxml')
        table_tags = soup.select('table')
        if table_tags:
            table_tag = table_tags[0]
            rows = parse_table(
                table=table_tag,
                col_names=col_names,
                callbacks=callbacks,
                extra_attrs={},
                head_tag=head_tag
            )
            processed_rows = transform_row(rows)
            return processed_rows
        return []
    except Exception as e:
        exception = cast_exception(e)
        raise exception


def process_xyyh_bhlccp(session: Session):
    # 本行理财产品
    resp_str = do_request(session,
                          PC_BHLCCP_REQUEST['method'],
                          PC_BHLCCP_REQUEST['url'],
                          PC_BHLCCP_REQUEST['headers'],
                          PC_BHLCCP_REQUEST['data'])
    return do_parse(resp_str,
                    ['pass', '募集方式', '产品名称', '银行业理财信息登记系统登记编码', '产品代码', '产品简称', '代销风险等级', '发行机构', 'pass', '产品类型'],
                    'td')


# 处理兴业银行其他理财产品(代销)的数据
def process_xyyh_qtlccp_dx(session: Session):
    resp_str = do_request(session, PC_QTLCCP_DX_REQUEST['method'], PC_QTLCCP_DX_REQUEST['url'],
                          PC_QTLCCP_DX_REQUEST['headers'], PC_QTLCCP_DX_REQUEST['data'])
    return do_parse(resp_str,
                    ['pass', '募集方式', '产品名称', '银行业理财信息登记系统登记编码', '产品代码', '产品简称', '代销风险等级', '发行机构', 'pass', '产品类型'],
                    'td')


# 处理兴业银行-理财产品代销
def process_xyyh_lccp_dx(session: Session):
    resp_str = do_request(session, PC_LCCP_DX_REQUEST['method'], PC_QTLCCP_DX_REQUEST['url'],
                          PC_LCCP_DX_REQUEST['headers'], PC_LCCP_DX_REQUEST['data'])
    return do_parse(resp_str,
                    ['pass', '募集方式', '产品名称', '银行业理财信息登记系统登记编码', '产品代码', '产品简称', '代销风险等级', '发行机构', 'pass', '产品类型'],
                    'td')


# 券商集合（代销）
def process_xyyh_qsjh_dx(session: Session):
    resp_str = do_request(session,
                          PC_QSJH_DX_REQUEST['method'],
                          PC_QSJH_DX_REQUEST['url'],
                          PC_QSJH_DX_REQUEST['headers'],
                          PC_QSJH_DX_REQUEST['data'])
    return do_parse(resp_str,
                    ['募集方式（公募/私募）', '产品名称', '产品代码', '产品风险等级', '产品类型', '发行机构', 'pass'],
                    'td'
                    )


# 结构性存款
def process_xyyh_jgxck(session: Session):
    resp_str = do_request(session,
                          PC_JGXCK_REQUEST['method'],
                          PC_JGXCK_REQUEST['url'],
                          PC_JGXCK_REQUEST['headers'],
                          PC_JGXCK_REQUEST['data'])
    return do_parse(resp_str=resp_str,
                    col_names=['pass', '产品名称', '产品代码', '风险等级', '发行机构', '预计到期利率（%）', '产品起息日'],
                    head_tag='td',
                    callbacks={
                        '预计到期利率（%）': lambda x: x + '（百分比）'
                    }
                    )


# 信托（代销）
def process_xyyh_xt_dx(session: Session):
    resp_str = do_request(session,
                          PC_XT_DX_REQUEST['method'],
                          PC_XT_DX_REQUEST['url'],
                          PC_XT_DX_REQUEST['headers'],
                          PC_XT_DX_REQUEST['data'])
    return do_parse(resp_str=resp_str,
                    col_names=['募集方式（公募/私募）', '产品名称', '产品代码', '产品风险等级', '产品类型', '发行机构', 'pass'],
                    head_tag='td',
                    )


# 保险（代销）
def process_xyyh_bx_dx(session: Session):
    resp_str = do_request(session,
                          PC_BX_DX_REQUEST['method'],
                          PC_BX_DX_REQUEST['url'],
                          PC_BX_DX_REQUEST['headers'],
                          PC_BX_DX_REQUEST['data'])
    return do_parse(resp_str=resp_str,
                    col_names=['pass', '产品名称', '产品代号', '产品风险级别', '产品类型', '发行机构', 'pass', '产品状态'],
                    head_tag='td',
                    )


def process_xyyh_pc(session: Session):
    bhlccp = process_xyyh_bhlccp(session)
    xt_dx = process_xyyh_xt_dx(session)
    lccp_dx = process_xyyh_lccp_dx(session)
    qsjh_dx = process_xyyh_qsjh_dx(session)
    bx_dx = process_xyyh_bx_dx(session)
    jgxck = process_xyyh_jgxck(session)
    qtlccp_dx = process_xyyh_qtlccp_dx(session)
    return bhlccp + xt_dx + lccp_dx + qsjh_dx + bx_dx + jgxck + qtlccp_dx


if __name__ == '__main__':
    sess = requests.session()
    process_xyyh_xt_dx(sess)
    sess.close()
