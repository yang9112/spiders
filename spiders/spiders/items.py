# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class DataItem(scrapy.Item):
    # define the fields for your item here like:
    type = scrapy.Field()
    medianame = scrapy.Field()
    pubtime = scrapy.Field()
    collecttime = scrapy.Field()
    title = scrapy.Field()
    abstract = scrapy.Field()
    source =scrapy.Field()
    url = scrapy.Field()
    content = scrapy.Field()