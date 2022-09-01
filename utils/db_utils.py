import sys

import pymysql
from global_config import init_oracle, DBType
from global_config import mysql_database, mysql_password, mysql_user, mysql_autocommit, mysql_host
from global_config import oracle_user, oracle_password, oracle_uri
import cx_Oracle as cx
from global_config import DB_ENV
from utils.string_utils import remove_space


def createInsertSql(properties: dict, db_type: DBType):
    """
        Return
            Fields,like `key1,key2,key3`
            Values,like `'value1','value2','value3'`
        used for create insert sql,like 'insert into tabName(%s) values(%s)'%(Fields,Values)
        :properties props: 字符串属性值的字典
        :return:
        """
    fields = ''
    values = ''
    for key in properties.keys():
        value = properties[key]
        fields += '%s,' % key
        values += "'%s'," % value
    fields = fields.strip(',')
    values = values.strip(',')
    return fields, values


def create_insert_sql_and_params(props_dict: dict, target_table: str, db_type: DBType = DB_ENV):
    """
    根据字典数据 创建insert sql以及cursor.execute需要传入的参数
    :param props_dict: 需要插入到数据库的字典
    :param target_table: 目标表
    :param db_type: 数据库类型(mysql或oracle)
    :return: 返回(sql,params)的元组
    """
    if db_type in (DBType.mysql, DBType.oracle):
        fields = ''
        values = ''
        params = []
        for k, v in props_dict.items():
            fields += f'{k},'
            values += f'%s,' if db_type == DBType.mysql else f':{k}'
            params.append(v)
        sql = f"""
            insert into {target_table}({fields.strip(',')})
            values({values.strip(',')})
        """
        return sql, tuple(params) if db_type == DBType.mysql else params


def create_update_sql_and_params(update_props: dict,
                                 constraint_props: dict,
                                 target_table: str,
                                 db_type: DBType = DB_ENV):
    set_str = ''
    where_str = ''
    params = []
    if not update_props:
        return
    for k, v in update_props.items():
        set_str += f'{k}=%s,' if db_type == DBType.mysql else f'{k}=:{k},'
        params.append(v)
    if constraint_props:
        for k, v in constraint_props.items():
            where_str += f'{k}=%s and ' if db_type == DBType.mysql else f'{k}=:{k} and '
            params.append(v)
        where_str = 'where ' + where_str.strip().strip('and')
    set_str = set_str.strip(',')
    sql = f"""
        update {target_table} set {set_str} {where_str}
    """
    return sql, tuple(params) if db_type == DBType.mysql else params


def insert_to_db(cursor, props_dict: dict, target_table: str, db_type: DBType = DB_ENV):
    """
    将字典数据插入到目标表中
    :param cursor:
    :param props_dict: 需要插入的字典数据
    :param target_table: 目标表
    :param db_type: 数据库类型(mysql或oracle)
    :return:
    """
    sql, params = create_insert_sql_and_params(props_dict, target_table, db_type)
    cursor.execute(sql, params)


def update_to_db(cursor,
                 update_props: dict,
                 constraint_props: dict,
                 target_table: str,
                 db_type: DBType = DB_ENV):
    """
    根据传入的需要更新的字段和约束条件进行更新
    :param cursor:
    :param update_props: 更新的字段
    :param constraint_props: 约束条件字段
    :param target_table: 目标表
    :param db_type: 数据库类型(mysql或oracle)
    :return:
    """
    sql, params = create_update_sql_and_params(update_props, constraint_props, target_table, db_type)
    cursor.execute(sql, params)


def get_conn_mysql(host=mysql_host, user=mysql_user, password=mysql_password, database=mysql_database):
    """
    连接mysql数据库
    :return: 返回连接mysql数据库的connection对象
    """
    connect = pymysql.connect(host=host,
                              user=user,
                              password=password,
                              database=database)
    connect.autocommit(mysql_autocommit)
    return connect


def get_conn_oracle(user=oracle_user, password=oracle_password, uri=oracle_uri):
    """
    连接oracle数据库
    :return: 返回节点oracle数据库的connection对象
    """
    try:
        init_oracle()
    except Exception as e:
        print(e)
    return cx.connect(user, password, uri)


def get_conn(db_type: DBType = DB_ENV):
    """
    获取数据库连接connection对象
    :param db_type: 数据库类型(mysql或oracle)
    :return:
    """
    if db_type == DBType.mysql:
        return get_conn_mysql()
    if db_type == DBType.oracle:
        return get_conn_oracle()


def close(objs: list):
    """
    根据传入的列表,针对列表中的对象,使用try except逐个调用close方法
    :param objs: 需要关闭的对象列表
    :return:
    """
    for obj in objs:
        if obj:
            try:
                obj.close()
            except Exception as e:
                print(e)


def create_table(
        data_dict: dict,
        str_type: str,
        number_type: str,
        table_name: str,
        cursor,
        sequence_name: str,
        trigger_name: str,
        db_type: DBType = DB_ENV):
    """
    根据dict字典创建表
    :param db_type:
    :param data_dict: 除了id\logId字段是数值类型外,其他的都是字符串类型,默认自动添加id、logId、createTime三个字段
    :param str_type:
    :param number_type:
    :param table_name:创建的表名
    :param cursor:连接数据库的指针
    :param sequence_name:当创建的是oracle表时,需要使用sequence创建自增主键
    :param trigger_name:当创建的是oracle表时,需要使用trigger创建自增主键
    :return:
    """
    if db_type == DBType.oracle:
        # 创建oracle数据库中的表（结尾处不能加';'否则建表失败）
        sql = f"""
            create table {table_name}(id {number_type},logId {number_type},
        """
        for k, _ in data_dict.items():
            if k in ['id', 'logId', 'createTime']:
                continue
            sql += f"{k} {str_type},"
        sql += f'createTime {str_type})'
        # 创建表
        try:
            cursor.execute(f"drop table {table_name}")
        except Exception as e:
            pass
        cursor.execute(sql)
        # 创建sequence(结尾处不能加';'否则创建报错)
        try:
            cursor.execute(f"drop sequence {sequence_name}")
        except Exception as e:
            pass
        sequence_sql = f"""
            create sequence {sequence_name}
            minvalue 1
            maxvalue 99999999
            start with 1
            increment by 1
            cache 50
        """
        cursor.execute(sequence_sql)
        # 创建trigger (结尾处的';'必须添加否则创建的trigger无效)
        try:
            cursor.execute(f"drop trigger {trigger_name}")
        except Exception as e:
            pass
        trigger_sql = f"""
            create or replace trigger {trigger_name}
                before insert
            on {table_name}
            for each row
            begin
                select {sequence_name}.nextval into :new.id from dual;
            end;
        """
        cursor.execute(trigger_sql)
        cursor.connection.commit()
    if db_type == DBType.mysql:
        # 创建mysql数据库中的表
        pass


def check_field_exists(field_name: str, table_name: str, cursor):
    """
    判断一张表是否存在指定的field字段
    :param field_name:
    :param table_name:
    :param cursor:
    :return: boolean表示目标字段在目标表中是否存在
    """
    try:
        sql = f"select {field_name} from {table_name}"
        cursor.execute(sql)
        cursor.fetchall()
        return True
    except Exception as e:
        return False


def add_field(field_name: str, data_type: str, table_name: str, cursor):
    """
    为指定表添加某个字段
    :param field_name:
    :param data_type:
    :param table_name:
    :param cursor:
    :return: boolean表示添加字段成功与否
    """
    try:
        sql = f"alter table {table_name} add {field_name} {data_type}"
        cursor.execute(sql)
        return True
    except Exception as e:
        return False


def check_table_exists(table_name, cursor):
    """
    检查数据库中是否存在指定表
    :param table_name:
    :param cursor:
    :return:boolean表示判断目标表是否存在
    """
    sql = f"select 1 from {table_name}"
    try:
        cursor.execute(sql)
        cursor.fetchall()
        return True
    except Exception as e:
        return False


def process_dict(data_dict: dict):
    """
    针对dict中value可能存在的'使用''进行替换以及去掉value中的空白字符串
    :param data_dict:
    :return:
    """
    row = {}
    for k, v in data_dict.items():
        if "'" in str(v):
            processed_v = remove_space(str(v).replace("'", "''"))
            row[k] = processed_v
        else:
            row[k] = v
    return row


__all__ = [
    "close",
    "get_conn",
    'create_table',
    'check_field_exists',
    'check_table_exists',
    'add_field',
    'process_dict',
    'insert_to_db',
    'update_to_db'
]

if __name__ == '__main__':
    conn = get_conn()
    cur = conn.cursor()
    create_table(data_dict={'id': 1, 'test': 'test'},
                 str_type='varchar2(100)',
                 number_type='number(11)',
                 table_name='test',
                 cursor=cur,
                 sequence_name='sequence_test',
                 trigger_name='trigger_test')
    cur.connection.commit()
    cur.execute("insert into test(test) values ('a')")
    cur.execute("select * from test")
    print(cur.fetchall())
    cur.execute("drop sequence sequence_test")
    cur.execute("drop trigger trigger_test")
    cur.execute("drop table test")
    cur.connection.commit()
