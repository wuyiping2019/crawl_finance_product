import enum
import logging
import mimetypes
import threading
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
import smtplib
from email import encoders
from email.header import Header
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
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
    MAIL_SMTP_CONNECTION_EXCEPTION = 3, '无法连接到邮件服务器'
    MAIL_SMTP_LOGIN_FAILED_EXCEPTION = 4, '邮箱认证失败'


class Mail:
    """
    发送邮件实体的总接口
    """

    @abstractmethod
    def send_mail(self, *args, **kwargs):
        pass


class MessageFactory:
    def create_smtp_message(self, sender, receivers, subject, content, attach_files):
        # 创建一个带附件的实例
        message = MIMEMultipart()
        message['From'] = sender
        message['To'] = COMMASPACE
        message['Subject'] = Header(subject, 'utf-8')
        # 邮件正文内容
        message.attach(MIMEText(content, 'plain', 'utf-8'))
        part = MIMEBase('application', 'octet-stream')
        # 此处的path是需要添加附件的文件路径
        for file in attach_files:
            ctype, encoding = mimetypes.guess_type(file)
            if ctype is None or encoding is not None:
                ctype = dtype
        part.set_payload(open(self.path, "rb").read())
        encoders.encode_base64(part)
        # filename表示附件中文件的命名
        part.add_header('Content-Disposition', 'attachment', filename=self.filename)
        message.attach(part)
        return message


class MailMailBySMTP(Mail):
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

    def __init__(self,
                 sender,
                 receivers,
                 server,
                 username,
                 password,
                 *args,
                 **kwargs):
        self.lock.acquire()
        try:
            if self._init_flag:
                pass
            else:
                # TODO 定义初始化逻辑
                self.sender = sender
                self.receivers = receivers
                self.server = server
                self.username = username
                self.password = password
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

    def get_smtp_obj(self):
        smtp_obj = smtplib.SMTP_SSL(self.server)
        try:
            smtp_obj.connect(self.server)
        except Exception as e:
            raise MailException(
                code=MailExceptionEnum.MAIL_SMTP_CONNECTION_EXCEPTION.code,
                msg=MailExceptionEnum.MAIL_SMTP_CONNECTION_EXCEPTION.msg,
                error=e
            )
        try:
            smtp_obj.login(user=self.username, password=self.password)
        except Exception as e:
            raise MailException(
                code=MailExceptionEnum.MAIL_SMTP_LOGIN_FAILED_EXCEPTION.code,
                msg=MailExceptionEnum.MAIL_SMTP_LOGIN_FAILED_EXCEPTION.msg,
                error=e
            )
        return smtp_obj

    def send_mail(self, sender, receivers, server, message):
        smtp_obj = self.get_smtp_obj()
        smtp_obj.sendmail(self.sender, self.receivers, message.as_string())

    def create_message(self):
        # 创建一个带附件的实例
        message = MIMEMultipart()
        message['From'] = self.sender
        message['To'] = self.receivers
        message['Subject'] = Header(self.mail_title, 'utf-8')
        # 邮件正文内容
        message.attach(MIMEText('请查收附件数据 from 弘康！', 'plain', 'utf-8'))
        part = MIMEBase('application', 'octet-stream')
        # 此处的path是需要添加附件的文件路径
        part.set_payload(open(self.path, "rb").read())
        encoders.encode_base64(part)
        # filename表示附件中文件的命名
        part.add_header('Content-Disposition', 'attachment', filename=self.filename)
        message.attach(part)
        return message


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
