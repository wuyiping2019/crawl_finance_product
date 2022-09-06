# Scrapy settings for scrapy_modules project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html
import os

BOT_NAME = 'scrapy_modules'

SPIDER_MODULES = ['scrapy_modules.spiders']
NEWSPIDER_MODULE = 'scrapy_modules.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider custome_middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'scrapy_modules.custome_middlewares.payh_middleware.PayhDownloaderMiddleware': 1,
# }

# Enable or disable downloader custome_middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    'scrapy_modules.custome_middlewares.ScrapyModulesDownloaderMiddleware': 543,
# }
DOWNLOADER_MIDDLEWARES = {
    'scrapy_modules.custome_middlewares.payh_middleware.PayhDownloaderMiddleware': 1,
    'scrapy_modules.custome_middlewares.zglcw_middleware.ZGLCWDownloaderMiddleware': 1,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy_project.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'scrapy_modules.pipelines.PayhPipeline': 300,
    'scrapy_modules.pipelines.ZGLCWPipeline': 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy_project.extensions.httpcache.FilesystemCacheStorage'
# 决定最大值
CONCURRENT_REQUESTS_PER_DOMAIN = 1
# 下面两个二选一，一个是针对域名设置并发，一个是针对IP设置并发
CONCURRENT_REQUESTS_PER_IP = 1

# logger
from datetime import datetime
import os

today = datetime.now()
log_file_path = f"scrapy_modules/log/scrapy_{today.year}_{today.month}_{today.day}.log"

LOG_LEVEL = 'DEBUG'
LOG_FILE = log_file_path
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, 'w', encoding='UTF-8') as f:
        f.write('--------创建日志文件-----------')
