#!/usr/bin/python
#-*-coding:utf-8-*-
from scrapy import Spider
from scrapy import Request
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.selector import Selector
from scrapy import log
from spiders.items import BingNewsItem 
from bs4 import BeautifulSoup
import time
import json,re
import sys
import urllib

reload(sys)
sys.setdefaultencoding('utf-8')

class BingNewSpider(Spider):
    name = "bingnew"
    domain_url = "http://cn.bing.com"
    start_urls = []

    def __init__ (self):
        super(BingNewSpider,self).__init__()
        #将final绑定到爬虫结束的事件上
        dispatcher.connect(self.initial,signals.engine_started)
        dispatcher.connect(self.finalize,signals.engine_stopped)
    
    def initial(self):
        self.log('---started----')
        self.getStartUrl()

    def finalize(self):
        self.log('---stopped---')
        #url持久化

    def getStartUrl(self):
        #从文件初始化查询关键词
        #过去24小时以及过去1小时的关键词
        #hour24 = 'qft=interval%3d"7"&form=PTFTNR'
        #hour1 = 'qft=interval%3d"4"&form=PTFTNR'
        with open("keywords.txt","r") as inputs:
            for line in inputs:
                self.start_urls.append(self.domain_url + '/news/search?q=' + urllib.quote(line))
    
    #一个回调函数中返回多个Request以及Item的例子
    def parse(self,response):
        #print '====start %s==' %response.url
        self.log('a response from %s just arrived!' %response.url)
        #抽取并解析新闻网页内容
        items = self.parse_items(response)
        #构造一个Xpath的select对象，用来进行网页元素抽取
        sel = Selector(response)
        requests = []
        
        for url in sel.xpath(u'//li/a[@class="sb_pagN"]/@href').extract():
            requests.append(self.make_requests_from_url(self.domain_url+url))
            
        for item in items:
            yield Request(url=item['url'], meta={'item': item}, callback=self.parse_content)
  
        #return requests
        for request in requests:
            yield request

    def parse_content(self,response):
        item = response.meta['item']
        if response.body:
            bsoup = BeautifulSoup(response.body,from_encoding='utf-8')
        item['content'] = bsoup.get_text()
        return item

    def parse_items(self,response):
        if response.body:
            bsoup = BeautifulSoup(response.body,from_encoding='utf-8')
        main_content = bsoup.select('div#SerpResult')[0]
        
        if main_content:
            elem_list = main_content.find_all('div', class_='sn_r')
        items = []
        if len(elem_list) > 0:
            for elem in elem_list:
                item = BingNewsItem()
                title = elem.find('div', 'newstitle')
                if title.a.get_text():
                    item['title'] = title.a.get_text()
                else:
                    continue
                item['url'] = title.a['href']
                author = elem.find('span',class_='sn_ST')
                if author:
                    #m = re.search('(\d{4}\/\d{1,2}\/\d{1,2})',source_time[0])
                    item['source'] = author.cite.get_text()
                    item['createTime'] = self.normalize_time(str(author.span.get_text()))
                if elem.find('span',class_='sn_snip'):
                    item['abstract']=elem.find('span',class_='sn_snip').get_text()
                items.append(item)
            return items
     
    def normalize_time(self, time_text):
        if re.match('\d{4}.*?\d{1,2}.*?\d{1,2}', time_text):
            time_text = time_text.replace('/', '-') + ' 00:00'
        else:
            #非标准时间转换为时间戳,再转为标准时间
            time_digit = float(filter(str.isdigit, time_text))
            
            interval = 0;
            if time_text.find('天') > 0 or time_text.find('day') > 0:
                interval = 86400
            elif time_text.find('时') > 0 or time_text.find('hour') > 0:
                interval = 3600
            elif time_text.find('分') > 0 or time_text.find('min') > 0:
                interval = 60
            elif time_text.find('秒') > 0 or time_text.find('second') > 0:
                interval = 1
            else:
                return time_text
            
            time_true = time.mktime(time.localtime()) - time_digit*interval
            time_text = time.strftime("%Y-%m-%d %H:%M", time.gmtime(time_true))

        return time_text