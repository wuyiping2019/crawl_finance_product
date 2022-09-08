from enum import Enum

import cx_Oracle
from cx_Oracle import DatabaseError


def raise_exception(error):
    # 如果报错属于oracle的报错
    if isinstance(error, cx_Oracle.DatabaseError):
        if error.args[0].code == 904:
            raise CustomException(1, 'oracle中执行插入操作,插入的字段不存在')
    else:
        raise error


def cast_exception(error):
    try:
        raise_exception(error)
    except CustomException as ce:
        return error
    except Exception as e:
        return e


class CustomException(Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg
