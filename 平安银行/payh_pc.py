import os
import re
import sys
import importlib
from requests import Session

from utils.crawl_request_utils import CrawlRequest
from utils.global_config import get_table_name
from utils.spider_flow import SpiderFlow, process_flow


def do_crawl(conn, session: Session, log_id: int, regex: str, process_func: callable):
    work_dir = os.path.dirname(__file__)
    files = os.listdir(work_dir)
    processed_rows = []
    mark_coding_log = {}
    for file in files:
        if re.findall(regex, file):
            module_name = process_func(file)
            temp_module = importlib.import_module(f'{module_name}', 'do_crawl')
            crawl = temp_module.do_crawl(session, conn)
            processed_rows += crawl.processed_rows
            mark_coding_log[module_name] = crawl.mark_code_mapping
    return processed_rows
