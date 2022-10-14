from typing import List

from bs4 import BeautifulSoup, Tag

from crawl_utils.string_utils import remove_space


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


def parse_table(table: Tag, col_names: list, callbacks: dict, extra_attrs: dict, head_tag: str = 'th') -> List[dict]:
    """

    :param table:
    :param col_names:
    :param callbacks: 回调函数 对value进行进一步的处理
    :param extra_attrs:
    :param head_tag
    :return:
    """
    table_head = table.select('tr')[0]
    heads = []
    trs = table_head.select(head_tag)
    for tr in trs:
        heads.append(remove_space(tr.text))
    # 该表单的列数
    cols = len(heads)
    trs = table.select('tr')
    if head_tag == 'tr':
        trs = trs[1:]
    rows = []
    for index, tr in enumerate(trs):
        row = []
        if index == 0:
            continue
        else:
            tds = tr.select('td')
            current_td_index = 0
            col_span = 1
            # 表单有多个列 一行就应该有多个个值
            for row_index in range(cols):
                if row_index == 0:
                    # 添加第一列的元素
                    current_td_index = 0
                    row.append(remove_space(tds[current_td_index].text))
                    # 记录当前td元素的colspan的值
                    col_span = int(tds[current_td_index]['colspan']) if check_attr(tds[current_td_index],
                                                                                   'colspan') else 1
                else:
                    # 如果col_span > 1表示上一列的元素占了多列
                    if col_span > 1:
                        row.append(remove_space(tds[current_td_index].text))
                        # 相应的col_span的值减一
                        col_span -= 1
                    else:
                        # 当col_span = 1时,表示需要向前一列取值
                        current_td_index += 1
                        row.append(remove_space(tds[current_td_index].text))
                        # 更新col_span
                        col_span = int(tds[current_td_index]['colspan']) if check_attr(tds[current_td_index],
                                                                                       'colspan') else 1
        row_dict = {}
        # 转换key
        for key, value in zip(col_names if col_names else heads, row):
            row_dict[key] = value
        # 删除pass的key
        if 'pass' in row_dict.keys():
            del row_dict['pass']
        # 转换value
        if callbacks:
            for key in callbacks.keys():
                if key in row_dict.keys():
                    callback = callbacks[key]
                    processed_value = str(callback(str(row_dict[key])))
                    row_dict[key] = processed_value
        if extra_attrs:
            row_dict.update(extra_attrs)
        rows.append(row_dict)
    return rows


if __name__ == '__main__':
    f = lambda x: x + '元'
    f1 = f('5')
    print(f1)
