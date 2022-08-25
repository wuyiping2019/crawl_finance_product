import re


def remove_space(string: str):
    """
    去掉字符串中的空白字符
    :param string:
    :return:
    """
    return re.sub(r'\s', '', string)
