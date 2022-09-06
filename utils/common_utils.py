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
