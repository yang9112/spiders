# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class BaiduNewsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
	title = scrapy.Field()
	createTime = scrapy.Field()
	abstract = scrapy.Field()
	source =scrapy.Field()
	url = scrapy.Field()
	content = scrapy.Field()
	#similarity_count=scrapy.Field()
 
class BingNewsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
	title = scrapy.Field()
	createTime = scrapy.Field()
	abstract = scrapy.Field()
	source =scrapy.Field()
	url = scrapy.Field()
	content = scrapy.Field()
	#similarity_count=scrapy.Field()
 
class SogouNewsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
	title = scrapy.Field()
	createTime = scrapy.Field()
	abstract = scrapy.Field()
	source =scrapy.Field()
	url = scrapy.Field()
	content = scrapy.Field()
	#similarity_count=scrapy.Field()