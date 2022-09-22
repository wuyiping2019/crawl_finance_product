import os
import importlib
from requests import Session
from utils.crawl_request_utils import CrawlRequest


def do_crawl(conn, session: Session) -> CrawlRequest:
    work_dir = os.path.dirname(__file__)
    files = os.listdir(work_dir)
    processed_rows = []
    mark_coding_log = {}
    crawl: CrawlRequest = None
    for file in files:
        if file.startswith('gfyh_pc_'):
            module_name = file.replace('.py', '')
            temp_module = importlib.import_module(f'{module_name}', 'do_crawl')
            crawl = temp_module.do_crawl(session, conn)
            processed_rows += crawl.processed_rows
            mark_coding_log[module_name] = crawl.mark_code_mapping
    if crawl:
        crawl.processed_rows = processed_rows
    return crawl
