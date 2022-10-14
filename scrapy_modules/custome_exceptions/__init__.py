# 定义各种可能出现的异常
from enum import Enum


class CustomeExceptionCode(Enum):
    # 解析中国理财网报错
    ZGLCWTableParseException = '600'
    # WebDriver获取网页总页数时报错
    PageNumCatchException = '601'


class PageNumCatchException(BaseException):
    msg = '无法获取总页码数'
    code = CustomeExceptionCode.PageNumCatchException.value


class ZGLCWTableParseException(BaseException):
    msg = '中国理财网数据解析错误'
    code = CustomeExceptionCode.ZGLCWTableParseException.value
