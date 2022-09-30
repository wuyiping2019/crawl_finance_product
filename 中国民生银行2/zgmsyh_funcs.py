import json
from utils.logging_utils import get_console_logger
from utils.string_utils import remove_space
import argparse

parser = argparse.ArgumentParser()
log_level = parser.get_default('log_level')
logger = get_console_logger()


def process_yjbjjz_pc(row: dict) -> None:
    """
    根据row的属性,获取业绩比较基准并设置到row上
    :param row:
    :return:
    """
    if 'divModesName' in row.keys():
        splits = remove_space(row.get('divModesName')).split(":")
        row['yjbjjz'] = json.dumps({
            'title': splits[1],
            'value': splits[0]
        })
    else:
        logger
