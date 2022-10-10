import json
from typing import List

import requests
from requests import Response

from config_parser import crawl_config
from crawl_utils.common_utils import delete_empty_value
from crawl_utils.crawl_request import CustomCrawlRequest, ConfigurableCrawlRequest
from crawl_utils.db_utils import getLocalDate
from 广发银行_完成.gfyh_assist import mobile_filter1, mobile_filter2, mobile_filter3
from 广发银行_完成.gfyh_config import MASK


class GfyhMobileCrawlRequest(ConfigurableCrawlRequest):
    def __init__(self):
        super(GfyhMobileCrawlRequest, self).__init__(
            name="广发银行PC端"
        )
        self.page_no = None
        self.gain_rs = None

    def _pre_crawl(self):
        pass

    def _config_params(self):
        pass

    def _parse_response(self, response: Response) -> List[dict]:
        pass

    def _config_end_flag(self):
        pass

    def _row_processor(self, row: dict) -> dict:
        pass

    def _row_post_processor(self, row: dict):
        pass
