# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy import Item, Spider
from scrapy.exceptions import DropItem

from scrapy_modules.items import ZGLCWItem, PayhItem
from scrapy_modules.spiders.payh_spider import PayhSpider
from utils.db_utils import get_conn_oracle, close
from utils.mark_log import insertLogToDB
from spider_config import LOG_TABLE_DETAIL, SpiderLogDetail
from utils.zglcw import updateZGLCWInfo


class PayhPipeline:
    def process_item(self, item: Item, spider: PayhSpider):
        # 当将item插入到数据库报错时,抛弃该条数据
        if isinstance(item,PayhItem):
            try:
                item_dict = {k: item.get(k) for k in item.keys()}
                insertLogToDB(cur=spider.cursor, properties=item_dict, log_table=spider.target_table)
                # 每插入一条数据都提交事务
                spider.cursor.connection.commit()
                spider.successItemCount += 1
            except Exception as e:
                # 抛出DropItem异常,系统会自动忽略这条Item
                # 手动记录抛出的数据
                insertLogToDB(cur=spider.cursor, properties={'error_msg': str(e)}, log_table=LOG_TABLE_DETAIL)
                spider.failureItemCount += 1
                spider.cursor.connection.commit()
                raise DropItem()


class ZGLCWPipeline:
    def process_item(self, item: Item, spider):
        if isinstance(item, ZGLCWItem):
            item_dict = {k: item.get(k) for k in item.keys()}
            updateZGLCWInfo(spider.cursor, spider.target_table, item_dict)
            spider.cursor.connection.commit()
