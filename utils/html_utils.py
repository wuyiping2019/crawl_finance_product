from bs4 import BeautifulSoup, Tag


def check_attr(tag: Tag, attr: str):
    """
    验证BeatifulSoup获取的Tag是否存在某个属性
    :param tag:
    :return:
    """
    flag = True
    try:
        tag[attr]
    except Exception as e:
        flag = False
    return flag
