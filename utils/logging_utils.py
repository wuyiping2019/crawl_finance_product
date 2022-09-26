import logging

import typing

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
    console_handle = logging.StreamHandler()
    logger.addHandler(console_handle)
    logger.setLevel(log_level)
    if module_name in log_modules:
        return logger
    else:
        return None
