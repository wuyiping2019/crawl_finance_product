import logging


def get_logger(fileName, loggerName):
    file_handler = logging.FileHandler(filename='./log/%s.log' % fileName,
                                       encoding='utf-8',
                                       mode='w')
    formatter = logging.Formatter("%(asctime)s - %(name)s-%(levelname)s %(message)s")
    file_handler.setFormatter(formatter)
    logger = logging.getLogger(loggerName)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    return logger
