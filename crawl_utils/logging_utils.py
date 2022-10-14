from __future__ import annotations
import logging
import threading
from config_parser import CrawlConfig
from crawl_utils.custom_exception import CustomException
from crawl_utils.db_utils import getLocalDate

"""
%(levelno)s: 打印日志级别的数值
%(levelname)s: 打印日志级别名称
%(pathname)s: 打印当前执行程序的路径，其实就是sys.argv[0]
%(filename)s: 打印当前执行程序名
%(funcName)s: 打印日志的当前函数
%(lineno)d: 打印日志的当前行号
%(asctime)s: 打印日志的时间
%(thread)d: 打印线程ID
%(threadName)s: 打印线程名称
%(process)d: 打印进程ID
%(message)s: 打印日志信息
"""
# 记录创建的所有logger对象
# logging.getLogger方法：相同的name获取的是同一个logger对象
# 但是为了避免重复调用get_logger导致重复调用addHandler方法造成日志重复的情况
# 将创建的logger对象以name:logger字典的形式保存在该变量中
# name单例对象
logger_dict = {}
crawl_config = CrawlConfig()


class CustomFilter(logging.Filter):
    def __init__(self, log_modules):
        super().__init__()
        self.log_modules = log_modules

    def filter(self, record: logging.LogRecord) -> bool:
        # 获取日志来自的模块名称
        module = record.filename.replace('.py', '')
        if (self.log_modules and module and module in self.log_modules) or '*' in self.log_modules:
            return True


def get_logger(name,
               log_level=crawl_config.log_level,
               log_modules=crawl_config.log_modules,
               filename=crawl_config.log_filename,
               **kwargs):
    if name in logger_dict.keys():
        return logger_dict[name]
    # 创建日志对象
    logger = logging.getLogger(name)
    # 创建日志的输出格式对象
    if 'formatter' not in kwargs:
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(threadName)s %(filename)s %(funcName)s %(message)s')
    else:
        formatter = logging.Formatter(kwargs['formatter'])
    # 创建日志过滤器对象
    custom_filter = CustomFilter(log_modules)
    # 创建控制台日志输出handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.addFilter(custom_filter)
    logger.addHandler(console_handler)
    # 创建文件日志输出handler
    if filename:
        file_handler = logging.FileHandler(filename=filename)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(custom_filter)
        logger.addHandler(file_handler)
    logger.setLevel(log_level)
    # 将logger对象放入logger_dict中
    logger_dict[name] = logger
    return logger


def log(logger: logging.Logger, level: str | int, where: str, msg: str):
    thread_name = threading.current_thread().name
    if logger:

        if level.lower() in ['debug'] or level == 10:
            logger.log(level=10, msg=f"{(thread_name + '-' + 'DEBUG').ljust(16)}:{getLocalDate()}:{where}:{msg}")
        elif level.lower() in ['info'] or level == 20:
            logger.log(level=20, msg=f"{(thread_name + '-' + 'INFO').ljust(16)}:{getLocalDate()}:{where}:{msg}")
        elif level.lower() in ['warn', 'warning'] or level == 30:
            logger.log(level=30, msg=f"{(thread_name + '-' + 'WARN').ljust(16)}:{getLocalDate()}:{where}:{msg}")
        elif level.lower() in ['error'] or level == 40:
            logger.log(level=40, msg=f"{(thread_name + '-' + 'ERROR').ljust(16)}:{getLocalDate()}:{where}:{msg}")
        else:
            raise CustomException(None, f"ERROR:{getLocalDate()}:logging_utils.log 调用失败")
