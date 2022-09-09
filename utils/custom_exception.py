import json
from enum import Enum

import cx_Oracle
from cx_Oracle import DatabaseError


def raise_exception(error, **kwargs):
    # 如果报错属于oracle的报错
    if isinstance(error, cx_Oracle.DatabaseError):
        if error.args[0].code == 904:
            raise CustomException(1, 'oracle中执行插入操作,插入的字段不存在')
    # json.loads报错
    elif isinstance(error, json.JSONDecodeError):
        if error.colno == 2:
            raise CustomException(2, '解析json异常')
        if error.colno == 1:
            raise CustomException(4, '解析的字符串并非json格式')
    # dict无法获取不存在的key值
    elif isinstance(error, KeyError):
        error_key = error.args[0]
        raise CustomException(3, f'字典不存在"{error_key}"的key值')
    # 字典无法获取某个key值
    else:
        raise error


def cast_exception(error, **kwargs):
    try:
        raise_exception(error, **kwargs)
    except Exception as e:
        return e


class CustomException(Exception):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

    def __repr__(self):
        return f'CustomException({self.code, self.msg})'

    def __str__(self):
        return f'CustomException({self.code, self.msg})'
