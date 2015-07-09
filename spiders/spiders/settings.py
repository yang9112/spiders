# -*- coding: utf-8 -*-

# Scrapy settings for spiders project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'spiders'

SPIDER_MODULES = ['spiders.spiders']
NEWSPIDER_MODULE = 'spiders.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 5.2) AppleWebKit/534.30 (KHTML, like Gecko) Chrome/12.0.742.122 Safari/534.30'

ITEM_PIPELINES = {
    'spiders.pipelines.JsonWriterPipeline': 300,
}

#the max depth of the spider
DEPTH_LIMIT = 2

#下载延迟
DOWNLOAD_DELAY = 0.5#500MS OF DELAY

#禁用cookies
COOKIES_ENABLED=False

#日志信息
LOG_FILE='./spider.log'
LOG_LEVEL='INFO'
LOG_STDOUT=True