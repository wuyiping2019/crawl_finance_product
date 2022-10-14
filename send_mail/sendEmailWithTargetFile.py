# -*- coding:utf-8 -*-
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import sys


class SendEmail:
    def __init__(self, sender, receivers, mail_title, paths, smtpserver, username, password):
        self.sender = sender
        self.receivers = receivers
        self.mail_title = mail_title
        self.paths = paths
        self.smtpserver = smtpserver
        self.username = username
        self.password = password

    def sendEmail(self):
        # 创建一个带附件的实例
        message = MIMEMultipart()
        message['From'] = self.sender
        message['To'] = ';'.join(self.receivers)
        message['Subject'] = Header(self.mail_title, 'utf-8')
        # 邮件正文内容
        message.attach(MIMEText('请查收附件数据 from 弘康！', 'plain', 'utf-8'))
        for path in self.paths:
            part = MIMEBase('application', 'octet-stream')
            # 此处的path是需要添加附件的文件路径
            part.set_payload(open(path, "rb").read())
            encoders.encode_base64(part)
            # filename表示附件中文件的命名
            part.add_header('Content-Disposition', 'attachment', filename=path.split("/")[-1])
            message.attach(part)
        smtpObj = smtplib.SMTP_SSL(self.smtpserver)
        smtpObj.connect(self.smtpserver)
        smtpObj.login(self.username, self.password)
        smtpObj.sendmail(self.sender, self.receivers, message.as_string())


def alert():
    print("请输入两个参数!")
    print("第一个参数：指定需要发送的用户邮箱,多个邮箱使用分号;分隔!")
    print("第二个参数：指定需要发送的数据文件路径,多个路径使用分号;分隔!")


if __name__ == '__main__':
    # 该脚本将数据发送给指定的用户
    if sys.argv[1] == '-h' or sys.argv[1] == '--help':
        alert()
    receivers = sys.argv[1].split(";")
    paths = sys.argv[2:]
    sender = 'from@hongkang-life.com'
    smtpserver = 'smtp.qiye.163.com'
    username = 'from@hongkang-life.com'
    password = '2017.com'
    yingyezongbu_email = SendEmail(sender=sender, receivers=receivers,
                                   mail_title='请查收数据！',
                                   paths=paths,
                                   smtpserver=smtpserver,
                                   username=username,
                                   password=password)
    yingyezongbu_email.sendEmail()
