# encoding:utf-8
import importlib
import sys
import os
import configparser
import threading
import time
from collections import OrderedDict
from logging import Logger
from typing import Optional

from dbutils.pooled_db import PooledDB

from utils.custom_exception import CustomException
from utils.db_utils import getLocalDate, get_db_poll
from utils.logging_utils import get_logger
from utils.string_utils import remove_space
import inspect

rootpath = os.path.dirname(__file__)
sys.path.extend([rootpath, ])


def parse_crawl_cfg() -> dict:
    config = configparser.ConfigParser()
    config.read('crawl.cfg', encoding='utf-8')
    config_dict = {}
    for section_name in config._sections.keys():
        ordered_dict: OrderedDict = config._sections[section_name]
        if section_name == 'thread':
            # 解析thread
            for key in ordered_dict.keys():
                try:
                    config_dict[f"thread.{key}"] = int(ordered_dict[key])
                except Exception as e:
                    raise CustomException(None, msg=f"无法解析thread:thread_num必须为INT类型")
        if section_name == 'pool.oracle':
            # 解析线程池
            try:
                user = remove_space(ordered_dict['user'])
                password = remove_space(ordered_dict['password'])
                dsn = remove_space(ordered_dict['dsn'])
                if user and password and dsn:
                    config_dict['db_type'] = 'oracle'
                    config_dict['pool.oracle.user'] = user
                    config_dict['pool.oracle.password'] = password
                    config_dict['pool.oracle.dsn'] = dsn
                    config_dict['poll_flag'] = True
            except Exception as e:
                pass

            if 'poll_flag' not in config_dict.keys() or not config_dict['poll_flag']:
                raise CustomException(None, f"配置数据库连接池失败")
        if section_name == 'poll.mysql':
            pass
        elif section_name == 'sleep':
            try:
                sleep_second = int(ordered_dict['sleep_second'])
                config_dict['sleep.sleep_second'] = sleep_second
            except Exception as e:
                raise CustomException(None, f"无法解析sleep:sleep_second必须为INT类型")
        elif section_name == 'crawl':
            if 'crawl' not in config_dict:
                config_dict['crawl'] = []
                config_dict['crawl.modules'] = []

            for key in ordered_dict.keys():
                config_dict['crawl'].append((key, ordered_dict[key]))
                config_dict['crawl.modules'].append(ordered_dict[key])
        elif section_name == 'logger':
            for key in ordered_dict.keys():
                config_dict[f'logger.{key}'] = ordered_dict[key]
    return config_dict


class MutiThreadCrawl:
    def __init__(self, crawl_queue: list, thread_num: int, poll: PooledDB, config_dict: dict, logger: Optional[Logger]):
        self.crawl_queue = crawl_queue
        self.lock = threading.RLock()
        self.thread_num = thread_num
        self.end_flag = False
        self.init_thread = thread_num
        self.poll = poll
        self.config_dict = config_dict
        self.logger = logger

    def crawl_thread(self, crawl_func):
        crawl_func()
        self.lock.acquire()
        if self.logger:
            self.logger.info(f"INFO:{getLocalDate()}:一个线程完成,当前运行线程数{self.init_thread - self.thread_num}个")
        self.thread_num -= 1
        self.lock.release()

    def get_crawl(self):
        new_thread_func = None
        self.lock.acquire()
        thread_flag = self.thread_num > 0
        crawl_queue_flag = len(self.crawl_queue) > 0
        if thread_flag and crawl_queue_flag:
            new_thread_func = self.crawl_queue.pop(0)
        if len(self.crawl_queue) == 0:
            self.end_flag = True
        self.thread_num -= 1
        self.lock.release()
        return new_thread_func

    def wrapper_crawl_func(self, crawl_func):
        crawl_func(self=self)
        self.lock.acquire()
        self.thread_num += 1
        self.lock.release()

    def run_crawl(self):
        while True:
            end_flag = False
            self.lock.acquire()
            try:
                if self.end_flag:
                    end_flag = self.end_flag
            except Exception as e:
                raise CustomException(None, f"ERROR:{getLocalDate()}:访问end_flag失败")
            finally:
                self.lock.release()

            if end_flag:
                break
            crawl_func = self.get_crawl()

            if self.logger:
                self.logger.info(f"INFO:{getLocalDate()}:准备启动一个线程爬取数据")
            # 启动一个爬取数据的线程
            thread = threading.Thread(target=self.wrapper_crawl_func, args=[crawl_func])
            thread.start()
            if self.logger:
                self.logger.info(f"INFO:{getLocalDate()}:成功启动一个线程,当前运行线程数{self.init_thread - self.thread_num}个")
            time.sleep(10)


if __name__ == '__main__':
    config_dict = parse_crawl_cfg()
    poll = get_db_poll()
    # 获取一个logger对象
    current_module_name = sys.modules[__name__].__name__  # __main__
    logger: Optional[Logger] = get_logger(log_name=config_dict['logger.name'],
                                          log_level=config_dict['logger.level'],
                                          log_modules=config_dict['logger.modules'],
                                          module_name=current_module_name)
    crawl_queue = []
    try:
        for crawl in config_dict['crawl']:
            try:
                crawl_module = importlib.import_module(crawl[0])
                for k, v in inspect.getmembers(crawl_module):
                    if k == crawl[1]:
                        crawl_queue.append(v)
                        continue
                thread_crawl = MutiThreadCrawl(crawl_queue=crawl_queue,
                                               thread_num=config_dict['thread.thread_num'],
                                               poll=poll,
                                               config_dict=config_dict,
                                               logger=logger
                                               )
                thread_crawl.run_crawl()
            except Exception as e:
                raise CustomException(None, f'无法导入{crawl[0]}模块')
    except Exception as e:
        raise e
    finally:
        if poll:
            if logger:
                logger.info(f"INFO:{getLocalDate()}:关闭数据连接池")
            poll.close()
