# encoding:utf-8
import importlib
import sys
import os
import threading
import time
import traceback
from logging import Logger
from crawl_utils.custom_exception import CustomException
import inspect
from config_parser import CrawlConfig
from crawl_utils.db_utils import check_table_exists
from crawl_utils.logging_utils import get_logger

rootpath = os.path.dirname(__file__)
sys.path.extend([rootpath, ])


class MutiThreadCrawl:
    def __init__(self, config: CrawlConfig, crawl_logger: Logger):
        self.lock = threading.RLock()
        self.__thread_num = 0
        self.config = config
        self.threads = []
        self.logger = crawl_logger

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
        # thread_func(*args)
        try:
            # 实际执行爬虫的方法
            thread_func(*args)
        except Exception as e:
            # 应该尽可能保证线程内部不出现异常
            logger.warn(traceback.format_exc())
        # 线程任务执行完毕 线程数 - 1
        self.set_thread_num(self.get_thread_num() - 1)
        self.logger.info(f'{threading.current_thread().name}线程完成,当前执行线程数:{self.get_thread_num()}')

    def daemon_thread(self):
        self.logger.info('启动守护线程')
        while True:
            if self.get_remaining_thread_num() <= 0 and self.get_thread_num() == 0:
                return
            if self.get_remaining_thread_num() > 0 and self.get_thread_num() < self.config.max_thread:
                thread = self.pop_thread()
                self.logger.info(f'守护线程启动{threading.current_thread().name}爬虫进程')
                # 启动一个线程 线程数 + 1
                thread.start()
                self.set_thread_num(self.get_thread_num() + 1)  # 线程数 + 1
            time.sleep(3)


if __name__ == '__main__':
    """
    核心逻辑：以多线程的方式调用配置在crawl.cfg中[crawl]区域中的模块
    [crawl]区域以module=func的方式进行配置
    调用func的时候会自动传入解析crawl.cfg的CrawlConfig对象(该对象的解析逻辑在config_parser模块中)
    """
    crawl_config = CrawlConfig()
    logger = get_logger(name=__name__)
    # 1.检测是否存在记录日志的表
    log_table_exists_flag = check_table_exists(crawl_config.log_table, crawl_config.db_pool)
    if not log_table_exists_flag:
        logger.error("无法检测到记录日志的表:%s" % crawl_config.log_table)
        logger.error(f"爬虫程序退出准备退出...")
        sys.exit(1)
    else:
        logger.info("检测到记录日志的表:%s" % crawl_config.log_table)
    muti_thread_crawl = MutiThreadCrawl(config=crawl_config, crawl_logger=logger)
    func_list = []
    try:
        # 将所有需要执行的函数放入crawl_queue列表中
        crawl_module_names = []
        for crawl_module, crawl_func_name in crawl_config.config['crawl'].items():
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

        for func, thread_name in zip(func_list, crawl_module_names):
            muti_thread_crawl.threads.append(threading.Thread(target=muti_thread_crawl.wrapper_func,
                                                              args=[func, crawl_config],
                                                              name=thread_name))
        daemon_thread = threading.Thread(target=muti_thread_crawl.daemon_thread, name='starter')
        daemon_thread.start()
        daemon_thread.join()

    except Exception as e:
        raise e
    finally:
        logger.info('所有线程爬虫线程执行完毕,准备退出爬虫程序...')
