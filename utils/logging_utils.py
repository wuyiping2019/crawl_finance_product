from __future__ import annotations

import logging
import threading

import typing

from utils.custom_exception import CustomException
from utils.db_utils import getLocalDate


# def get_logger(fileName, loggerName):
#     file_handler = logging.FileHandler(filename=fileName,
#                                        encoding='utf-8',
#                                        mode='w')
#     formatter = logging.Formatter("%(asctime)s - %(name)s-%(levelname)s %(message)s")
#     file_handler.setFormatter(formatter)
#     logger = logging.getLogger(loggerName)
#     logger.addHandler(file_handler)
#     logger.setLevel(logging.INFO)
#     return logger


def get_logger(log_name: str, log_level: str, log_modules: list, module_name: str) -> typing.Optional[logging.Logger]:
    logger = logging.getLogger(log_name)
    logger.setLevel(log_level)
    if not logger.handlers:
        console_handle = logging.StreamHandler()
        logger.addHandler(console_handle)
    if module_name in log_modules:
        return logger
    else:
        return None


def log(logger: logging.Logger, level: str | int, where: str, msg: str):
    thread_name = threading.current_thread().name
    if logger:

        if level.lower() in ['debug'] or level == 10:
            logger.log(level=10, msg=f"{(thread_name +'-' + 'DEBUG').ljust(16)}:{getLocalDate()}:{where}:{msg}")
        elif level.lower() in ['info'] or level == 20:
            logger.log(level=20, msg=f"{(thread_name + '-' + 'INFO').ljust(16)}:{getLocalDate()}:{where}:{msg}")
        elif level.lower() in ['warn', 'warning'] or level == 30:
            logger.log(level=30, msg=f"{(thread_name + '-' + 'WARN').ljust(16)}:{getLocalDate()}:{where}:{msg}")
        elif level.lower() in ['error'] or level == 40:
            logger.log(level=40, msg=f"{(thread_name + '-' + 'ERROR').ljust(16)}:{getLocalDate()}:{where}:{msg}")
        else:
            raise CustomException(None, f"ERROR:{getLocalDate()}:logging_utils.log 调用失败")
