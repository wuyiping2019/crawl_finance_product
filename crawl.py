# encoding:utf-8
import importlib
import sys
import os
import threading
import time
from logging import Logger
from typing import Optional

from dbutils.pooled_db import PooledDB

from utils.custom_exception import CustomException
from utils.db_utils import get_db_poll
from utils.logging_utils import get_logger, log
import inspect
from arg_parser import config_dict,
rootpath = os.path.dirname(__file__)
sys.path.extend([rootpath, ])


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
    logger_config = config_dict['logger']
    logger: Optional[Logger] = get_logger(log_name=logger_config['name'],
                                          log_level=logger_config['level'],
                                          log_modules=logger_config['modules'],
                                          module_name=current_module_name)
    func_list = []
    try:
        # 将所有需要执行的函数放入crawl_queue列表中
        crawl_module_names = []
        for crawl_module, crawl_func_name in config_dict['crawl'].items():
            threads = []
            try:
                crawl_module_names.append(crawl_module)
                crawl_module = importlib.import_module(crawl_module)
                for k, v in inspect.getmembers(crawl_module):
                    if k == crawl_func_name:
                        func_list.append(v)
                        continue
            except Exception as e:
                raise CustomException(None, f'无法导入{crawl_module}模块')
        muti_thread_crawl = MutiThreadCrawl(max_thread=config_dict['thread']['thread_num'],
                                            poll=poll,
                                            config_dict=config_dict,
                                            logger=logger)
        for func, thread_name in zip(func_list, crawl_module_names):
            muti_thread_crawl.threads.append(threading.Thread(target=muti_thread_crawl.wrapper_func,
                                                              args=[func, muti_thread_crawl],
                                                              name=thread_name))
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
