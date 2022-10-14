import argparse
import configparser
import enum
import functools
import logging
import os
import threading
import typing
from concurrent.futures import ThreadPoolExecutor

from dbutils.pooled_db import PooledDB
from crawl_utils.db_utils import get_db_poll
from crawl_utils.global_config import DBType
import atexit

"""
解析crawl.cfg配置文件
解析结果以CrawlConfig对象的形式返回
"""
# 创建日志对象
logger = logging.getLogger(__name__)
# 创建日志的输出格式对象
formatter = logging.Formatter('%(asctime)s %(levelname)s %(threadName)s %(filename)s %(funcName)s %(message)s')
# 创建控制台日志输出handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logger.setLevel("DEBUG")


class ConfigParserException(Exception):
    def __init__(self, code, msg, **kwargs):
        super(ConfigParserException, self).__init__()
        self.code = code
        self.msg = msg


class ConfigParserExceptionEnum(enum.Enum):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

    POOL_DB_INITIALIZE_EXCEPTION = 1, '初始化数据库连接池失败'


def cast_data(value, value_type):
    if value_type == 'int':
        value = int(value)
    if value_type == 'str':
        value = str(value)
    if value_type == 'bool':
        if value == 'True':
            value = True
        elif value == 'False':
            value = False
    if value_type == 'list':
        return value
    return value


def parse_crawl_cfg() -> dict:
    f"""
    解析项目根目录下的crawl.cfg配置文件
    :return: 返回一个字典,以"section:dict"的形式返回,其中dict是每个配置区域都是一个字典形式
    """
    config = configparser.ConfigParser()
    current_work_dir = os.path.dirname(__file__)
    config.read(os.path.join(current_work_dir, 'crawl.cfg'), encoding='utf-8')
    config_dict = {}
    for section_name in config._sections.keys():
        ordered_dict = config._sections[section_name]
        section_dict = {}
        for key in ordered_dict.keys():
            value = ordered_dict[key]
            if ',' in value:
                split = value.split(',')
                value = split[0] if len(split[0:-1]) == 1 else split[0:-1]
                value_type = split[-1]
                value = cast_data(value, value_type)
            section_dict[f"{key}"] = value
        config_dict[section_name] = section_dict
    logger.info("解析crawl.cfg:%s" % config_dict)
    return config_dict


def parse_command_args(config: dict) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--level', type=str, help='配置日志级别')
    args = parser.parse_args()
    logger_level = args.level
    if logger_level:
        config['logger'].update({'level': logger_level})
    logger.info(f"解析命令行--level:%s" % logger_level)


def init_db_poll(config: dict, db_type: str) -> typing.Optional[PooledDB]:
    """
    根据数据库的类型 获取数据库连接池
    :param config:
    :param db_type:
    :return:
    """
    pool = None
    if db_type == "oracle":
        db_type = DBType.oracle
        pool = get_db_poll(db_type, **config['pool.oracle'])
    elif db_type == 'mysql':
        db_type = DBType.mysql
        pool = get_db_poll(db_type, **config['pool.mysql'])
    logger.info(f"完成数据库连接池初始化")
    return pool


def init_obj(config: dict) -> None:
    # 1.初始化数据库连接池
    # 1.1 获取需要初始化的数据库类型
    pool_type = config['pool.activate']['activate']
    try:
        pool = init_db_poll(config, pool_type)
        config['db_pool'] = pool
    except Exception as e:
        raise ConfigParserException(
            code=ConfigParserExceptionEnum.POOL_DB_INITIALIZE_EXCEPTION.code,
            msg=ConfigParserExceptionEnum.POOL_DB_INITIALIZE_EXCEPTION.msg,
            error=e
        )


def synchronized(func):
    func.__lock__ = threading.Lock()

    def lock_func(*args, **kwargs):
        with func.__lock__:
            return func(*args, **kwargs)

    return lock_func


class CrawlConfig:
    _instance = None
    _init_flag = False

    @synchronized
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
        return cls._instance

    @synchronized
    def __init__(self):
        if not self._init_flag:
            config = self.parse_config()
            self.config = config
            self.db_pool = config['db_pool']
            self.log_level = config['logger']['level']
            self.log_modules = config['logger']['modules']
            self.max_thread = config['thread']['max_thread']
            self.log_filename = config['logger'].get('filename', None)
            self.db_type: DBType = DBType.oracle \
                if config['pool.activate']['activate'] == 'oracle' \
                else DBType.mysql
            self.state = config['development']['state']
            self.log_table = config['log_table']['name']
            self._init_flag = True

    @classmethod
    def close(cls):
        db_poll = None
        try:
            db_poll = cls._instance.db_pool
        except Exception:
            pass
        if db_poll:
            db_poll.close()
            logger.info(f"成功关闭数据库连接池")

    def __repr__(self):
        return "CrawlConfig(config:%s,db_poll:%s,log_level:%s,max_thread:%s)" % (
            self.config, self.db_pool, self.log_level, self.max_thread)

    @staticmethod
    def parse_config():
        # 1.解析crawl.cfg配置文件
        try:
            cfg = parse_crawl_cfg()
            logger.info("成功解析crawl.cfg文件")
        except Exception as e:
            logger.exception("解析crawl.cfg配置文件失败")
            raise e
        # 2.解析命令行参数
        try:
            parse_command_args(cfg)
            logger.info("成功解析命令行参数")
        except Exception as e:
            logger.exception("解析命令行参数失败")
            raise e
        # 3.初始化必要的对象
        try:
            init_obj(cfg)
            logger.info("成功初始化对象")
        except Exception as e:
            logger.exception("初始化对象失败")
            raise e
        return cfg


def exitfunc():
    logger.info("程序结束回调函数,释放连接池资源")
    CrawlConfig.close()
    logger.info("在程序结束之前成功释放连接池资源")


# 程序结束之前的回调函数
atexit.register(exitfunc)

__all__ = ['CrawlConfig']

if __name__ == '__main__':
    """
    测试 CrawlConfig是线程安全的单例对象(只初始化一次-__init__方法仅执行一次)
    """


    def run_job(num):
        crawl_config = CrawlConfig()
        print("Thread:%s ID:%s POOL:%s" % (num, id(crawl_config), id(crawl_config.db_pool)))
        print('\n')


    with ThreadPoolExecutor(max_workers=10) as pool:
        for line in range(50):
            pool.submit(run_job, line)
