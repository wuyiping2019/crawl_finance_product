import enum
import logging
import threading
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor

from send_mail_config import send_mail_log_formatter, send_mail_log_level

logger = logging.getLogger(__name__)
formatter = logging.Formatter(fmt=send_mail_log_formatter)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(level=send_mail_log_level)


class MailException(Exception):
    def __init__(self, code, msg, **kwargs):
        self.code = code
        self.msg = msg
        for k, v in kwargs:
            setattr(self, k, v)


class MailExceptionEnum(enum.Enum):
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

    MAIL_UTILS_INSTANTIATION_EXCEPTION = 1, 'MailUtils实例化失败'
    MAIL_UTILS_INITIALIZATION_EXCEPTION = 2, 'MailUtils对象初始化失败'


class Mail:
    """
    发送邮件实体的总接口
    """

    @abstractmethod
    def send_mail(self):
        pass


class MailUtils(Mail):
    """
    实现发送邮件的功能类：线程安全的单例对象
    """
    # 存储实例化对象
    _instance = None
    # 保证存储的实例化对象仅被初始化一次
    _init_flag = False
    # 定义一个lock
    lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """
        保证__new__方法只会创建一次对象
        :param args:
        :param kwargs:
        """
        cls.lock.acquire()
        try:
            if not cls._instance:
                # 如果_instance没有被创建
                logger.info('实例化MailUtils对象')
                cls._instance = super().__new__(cls)
            else:
                return cls._instance
        except Exception as e:
            raise MailException(
                code=MailExceptionEnum.MAIL_UTILS_INSTANTIATION_EXCEPTION.code,
                msg=MailExceptionEnum.MAIL_UTILS_INSTANTIATION_EXCEPTION.msg,
                error=e
            )
        finally:
            cls.lock.release()

    def __init__(self, *args, **kwargs):
        self.lock.acquire()
        try:
            if self._init_flag:
                pass
            else:
                # TODO 定义初始化逻辑
                logger.info('初始化MailUtils对象')
                self._init_flag = True
        except Exception as e:
            raise MailException(
                code=MailExceptionEnum.MAIL_UTILS_INITIALIZATION_EXCEPTION.code,
                msg=MailExceptionEnum.MAIL_UTILS_INITIALIZATION_EXCEPTION.msg,
                error=e
            )
        finally:
            self.lock.release()

    def send_mail(self):
        pass


#############################################################################
"""
以下为测试方法：
"""


def test_mail_utils_thread_safety():
    # 测试MailUtils线程安全
    def run_test(num):
        utils = MailUtils()
        print(f'线程id:{num}-对象id:{id(utils)}')

    with ThreadPoolExecutor(max_workers=10) as pool:
        for num in range(50):
            pool.submit(run_test, num)


#############################################################################


if __name__ == '__main__':
    test_mail_utils_thread_safety()  # 日志输出MailUtils仅实例化和初始化一次并且所有线程的MailUtils对象id相同
