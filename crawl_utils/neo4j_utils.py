import logging
import functools
import os.path

import numpy as np
from pandas import DataFrame

from neo4j import GraphDatabase, Result

neo4j_url = "bolt://10.2.13.150:7687"
username = "stage"
password = "User123$"
local_path = os.path.dirname(__file__)
dir_path = os.path.dirname(local_path)
filename = os.path.join(dir_path, 'log/neo4j.log')

file_handler = logging.FileHandler(filename=filename,
                                   encoding='utf-8',
                                   mode='w')
formatter = logging.Formatter("%(asctime)s - %(name)s-%(levelname)s %(message)s")
file_handler.setFormatter(formatter)
logger = logging.getLogger('neo4j')
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)


class Neo4jException(BaseException):
    def __init__(self, msg):
        self.msg = msg


def get_graph(neo4j_url, username, password):
    return GraphDatabase.driver(neo4j_url, auth=(username, password))


def create_insurance_company_node(tx, company_name) -> int:
    create_node_cypher = "create (p:InsuranceCompany {name:$company_name}) return id(p)"
    rs: Result = tx.run(create_node_cypher, company_name=company_name)
    logger.info(create_node_cypher)
    ids = list(np.array(rs.values()).flatten())
    if len(ids) != 1:
        logger.warning(str(ids))
        raise Neo4jException('插入InsuranceCompany数据异常')
    logger.info("id(p):" + str(ids))
    return ids


def search_insurance_company_node(tx, company_name):
    search_node_by_name = "match (p:InsuranceCompany) where p.name = $company_name return id(p)"
    rs: Result = tx.run(search_node_by_name, company_name=company_name)
    ids = list(np.array(rs.values()).flatten())
    return ids


def create_insurance_product_node(tx, product_name, company_name):
    create_node_cypher = "create (p:InsuranceProduct {name:$product_name,company:$company_name}) return id(p)"
    rs: Result = tx.run(create_node_cypher, product_name=product_name, company_name=company_name)
    ids = list(np.array(rs.values()).flatten())
    return ids


def search_insurance_product_node(tx, product_name, company_name):
    search_node_by_name = "match (p:InsuranceProduct) " \
                          "where p.name = $product_name " \
                          "and p.company = $company_name" \
                          " return id(p)"
    rs: Result = tx.run(search_node_by_name, product_name=product_name, company_name=company_name)
    ids = list(np.array(rs.values()).flatten())
    return ids


def check_relation_beween_company_and_product(tx, company_name, product_name):
    check_relation_cypher = """match (p1:InsuranceCompany {name:$company_name})
                                      -[r1:保险产品]->
                                      (p2:InsuranceProduct {name:$product_name,company:$company_name}) 
                                match (p2:InsuranceProduct {name:$product_name,company:$company_name})
                                      -[r2:保险公司]->
                                      (p1:InsuranceCompany {name:$company_name}) 
                                return id(r1),id(r2)
                                """
    rs: Result = tx.run(check_relation_cypher,
                        company_name=company_name,
                        product_name=product_name)
    ids = list(np.array(rs.values()).flatten())
    return ids


def create_relation_beween_company_and_product(tx, company_name, product_name):
    create_relation_cypher = """match (p1:InsuranceCompany {name:$company_name}),
                                      (p2:InsuranceProduct {name:$product_name,company:$company_name}) 
                                create (p1)-[r1:保险产品]->(p2),(p2)-[r2:保险公司]->(p1)
                                return id(r1),id(r2)
                                """
    rs: Result = tx.run(create_relation_cypher, company_name=company_name, product_name=product_name)
    ids = list(np.array(rs.values()).flatten())
    return ids


# 创建CompanyType {name:"保险公司"}
def create_insurance_company_root_node(tx, company_type):
    create_root_node_cypher = """
        create (p:CompanyType {name:$company_type})
        return id(p)
    """
    logger.info(create_root_node_cypher)
    rs: Result = tx.run(create_root_node_cypher, company_type=company_type)
    ids = list(np.array(rs.values()).flatten())
    if len(ids) != 1:
        logger.error("创建保险公司根节点失败")
        raise Neo4jException("创建保险公司根节点失败")


# 检查CompanyType {name:"保险公司"}
def check_insurance_company_root_node(tx, company_type):
    check_root_node_cypher = """
            match (p:CompanyType {name:$company_type})
            return id(p)
        """
    rs: Result = tx.run(check_root_node_cypher, company_type=company_type)
    ids = list(np.array(rs.values()).flatten())
    return ids


# 创建CompanyType和InsuranceCompany之间的关系
def create_relation_between_companyType_and_insuranceCompany(tx, company_name):
    create_cypher = """
        match (p:CompanyType),(q:InsuranceCompany)
        where p.name = '保险公司'
        and q.name = $company_name
        create (p)-[r:保险公司]->(q)
        return id(r)
    """
    logger.info(create_cypher)
    rs: Result = tx.run(create_cypher, company_name=company_name)
    ids = list(np.array(rs.values()).flatten())
    if len(ids) != 0:
        logger.error("创建CompanyType和InsuranceCompany之间的关系失败")
        raise Neo4jException("创建CompanyType和InsuranceCompany之间的关系失败")


def check_relation_between_companyType_and_insuranceCompany(tx, company_name):
    check_cypher = """
        match (p:CompanyType)-[r:保险公司]->(q:InsuranceCompany)
        where p.name = '保险公司'
        and q.name = $company_name
        return id(r)
    """
    rs: Result = tx.run(check_cypher, company_name=company_name)
    ids = list(np.array(rs.values()).flatten())
    return ids


def add_detail_to_insurance_product(tx, company_name, product_name, props: dict):
    """
    将insurance_product_detail表中的属性写到InsuranceProduct上
    :param tx:
    :param company_name:
    :param product_name:
    :return:
    """
    add_detail_cypher = """
        match (p:InsuranceProduct)
        where p.name = $product_name
        and p.company = $company_name
    """
    set_str = ''
    for k, v in props.items():
        set_str += 'p.%s = $%s, ' % (k, k)
    set_str = set_str.strip(", ")
    add_detail_cypher = add_detail_cypher + 'set ' + set_str
    try:
        tx.run(add_detail_cypher,
               product_name=product_name,
               company_name=company_name,
               **props)
    except Exception as e:
        logger.error(str(e))
        raise Neo4jException("保险产品添加属性失败")


if __name__ == '__main__':
    pass
