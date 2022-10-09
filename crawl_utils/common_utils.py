import re


def transform_rows(origin_rows: list, key_mappings: dict, callbacks: dict, ignore_attrs: list, extra_attrs: dict):
    """

    :param extra_attrs: 额外需要添加的固定属性
    :param ignore_attrs: origin_rows中需要忽略的属性
    :param origin_rows: 一个dict的列表
    :param key_mappings: 针对origin_rows中字典的key进行转换
    :param callbacks: 针对转换之后的key对应的value值进行转换
    :return: 转换之后的dict列表
    """
    rows = []
    keys_in_mapping = key_mappings.keys()
    keys_in_callback = callbacks.keys()
    for origin_row in origin_rows:
        row = {}
        for key in origin_row.keys():
            if key in ignore_attrs:
                continue
            if key in keys_in_mapping:
                # 转换key值
                row[key_mappings[key]] = origin_row[key]
            else:
                row[key] = origin_row[key]
        for key in keys_in_callback:
            # 针对row中的value进行转换
            row[key] = callbacks[key](row[key])
        if extra_attrs:
            row.update(extra_attrs)
        rows.append(row)
    return rows


def delete_empty_value(kv: dict):
    """
    删除一个字典中value为空的key
    :param kv:
    :return:
    """
    keys = [key for key in kv.keys()]
    for key in keys:
        if not kv[key]:
            del kv[key]


def extract_bracket_content(info_str):
    pattern = re.compile(r'[{](.*)[}]', re.S)
    findall = re.findall(pattern, info_str)
    if findall:
        return findall[0]
    else:
        return ''


def extract_content_between_content(info_str, left_info, right_info):
    pattern = re.compile(r'%s(.+?)%s' % (left_info, right_info))
    findall = re.findall(pattern, info_str)
    if findall:
        return findall[0].replace(left_info, '').replace(right_info, '')
    else:
        return ''


if __name__ == '__main__':
    content = extract_content_between_content('根据华夏理财产品风险评级，本产品为 PR2级（中低风险） 理财产品。', '本产品为', '理财产品')
    print(content)
