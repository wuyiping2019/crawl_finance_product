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
from utils.db_utils import get_db_poll
from utils.logging_utils import get_logger, log
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
    def __init__(self, max_thread: int, poll: PooledDB, config_dict: dict, logger: Optional[Logger]):
        self.lock = threading.RLock()
        self.__thread_num = 0
        self.max_thread = max_thread
        self.poll = poll
        self.config_dict = config_dict
        self.logger = logger
        self.threads = []

    def get_thread_num(self) -> int:
        thread_num = 0
        self.lock.acquire()
        try:
            thread_num = self.__thread_num
        except Exception as e:
            pass
        finally:
            self.lock.release()
        return thread_num

    def set_thread_num(self, num):
        self.lock.acquire()
        try:
            self.__thread_num = num
        except Exception as e:
            pass
        finally:
            self.lock.release()

    def get_remaining_thread_num(self):
        self.lock.acquire()
        remaining_thread_num = None
        try:
            remaining_thread_num = len(self.threads)
        except Exception as e:
            pass
        finally:
            self.lock.release()
        return remaining_thread_num

    def pop_thread(self):
        self.lock.acquire()
        thread = None
        try:
            thread = self.threads.pop(0)
        except Exception as e:
            pass
        finally:
            self.lock.release()
        return thread

    def wrapper_func(self, thread_func, *args):
        thread_func(*args)
        # 线程任务执行完毕 线程数 - 1
        self.set_thread_num(self.get_thread_num() - 1)
        log(self.logger, 'info', where, f'{threading.current_thread().name}线程完成,当前执行线程数:{self.get_thread_num()}')

    def daemon_thread(self):
        log(self.logger, 'info', '__main__.daemon_thread', '启动守护线程')
        while True:
            if self.get_remaining_thread_num() <= 0 and self.get_thread_num() == 0:
                return
            if self.get_remaining_thread_num() > 0 and self.get_thread_num() < self.max_thread:
                thread = self.pop_thread()
                log(self.logger, 'info', '__main__.daemon_thread', f'守护线程启动{threading.current_thread().name}爬虫进程')
                # 启动一个线程 线程数 + 1
                thread.start()
                self.set_thread_num(self.get_thread_num() + 1)  # 线程数 + 1
            time.sleep(3)


if __name__ == '__main__':
    where = 'crawl.__main__'
    config_dict = parse_crawl_cfg()
    poll = get_db_poll()
    # 获取一个logger对象
    current_module_name = sys.modules[__name__].__name__  # __main__
    logger: Optional[Logger] = get_logger(log_name=config_dict['logger.name'],
                                          log_level=config_dict['logger.level'],
                                          log_modules=config_dict['logger.modules'],
                                          module_name=current_module_name)
    func_list = []
    try:
        # 将所有需要执行的函数放入crawl_queue列表中
        for crawl in config_dict['crawl']:
            threads = []
            try:
                crawl_module = importlib.import_module(crawl[0])
                for k, v in inspect.getmembers(crawl_module):
                    if k == crawl[1]:
                        func_list.append(v)
                        continue
            except Exception as e:
                raise CustomException(None, f'无法导入{crawl[0]}模块')
        muti_thread_crawl = MutiThreadCrawl(max_thread=config_dict['thread.thread_num'],
                                            poll=poll,
                                            config_dict=config_dict,
                                            logger=logger)
        for func, thread_name in zip(func_list, config_dict['crawl']):
            muti_thread_crawl.threads.append(threading.Thread(target=muti_thread_crawl.wrapper_func,
                                                              args=[func, muti_thread_crawl],
                                                              name=thread_name[0]))
        daemon_thread = threading.Thread(target=muti_thread_crawl.daemon_thread, name='starter')
        daemon_thread.start()
        daemon_thread.join()
    except Exception as e:
        raise e
    finally:
        log(logger, 'info', where, '所有线程爬虫线程执行完毕,准备关闭数据库连接池')
        if poll:
            log(logger, 'info', where, '关闭数据库连接池')
            poll.close()
            log(logger, 'info', where, '成功关闭数据库连接池')
