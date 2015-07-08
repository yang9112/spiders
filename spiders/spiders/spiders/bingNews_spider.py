#!/usr/bin/python
#-*-coding:utf-8-*-
from scrapy import Spider
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.selector import Selector
from scrapy import log
from spiders.items import BingNewsItem 
from bs4 import BeautifulSoup
import json,re
import sys
import urllib

reload(sys)
sys.setdefaultencoding('utf-8')

class BaiduNewSpider(Spider):
    name = "bingnew"
    domain_url = "http://cn.bing.com"
    headers = {
        'User-Agent':'Mozilla/5.0 (Windows NT 5.2) AppleWebKit/534.30 (KHTML, like Gecko) Chrome/12.0.742.122 Safari/534.30'    
    }
    start_urls = []
    
    def __init__ (self):
        super(BaiduNewSpider,self).__init__()
        #将final绑定到爬虫结束的事件上
        dispatcher.connect(self.initial,signals.engine_started)
        dispatcher.connect(self.finalize,signals.engine_stopped)
    
    def initial(self):
        self.log('---started----')
        #init the depth
        self.depth = 0
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
        #抽取搜索结果页详细页面链接
        urls = sel.xpath(u'//div[@class="newstitle"]/a/@href').extract()
        requests = []
        for url in urls:
            requests.append(self.make_requests_from_url(url).replace(callback=self.parse_content))
        
        #测试设置最多2层
        if(self.depth < 2):
            self.depth = self.depth + 1
            for url in sel.xpath(u'//li/a[@class="sb_pagN"]/@href').extract():
                requests.append(self.make_requests_from_url(self.domain_url+url))
            
        for item in items:
            yield item
        #return requests
        for request in requests:
            yield request

    def parse_content(self,response):
        item = BingNewsItem()
        item['url'] = response.url
        if response.body:
            bsoup = BeautifulSoup(response.body,from_encoding='utf-8')
        item['content'] = bsoup.get_text()
        yield item

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
                    item['createTime'] = author.span.get_text()
                if elem.find('span',class_='sn_snip'):
                    item['abstract']=elem.find('span',class_='sn_snip').get_text()
                items.append(item)
            return items