#!/usr/bin/python
#-*-coding:utf-8-*-
from scrapy import Spider
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.selector import Selector
from scrapy import log
from spiders.items import SogouNewsItem 
from bs4 import BeautifulSoup
import json,re
import sys
import urllib

reload(sys)
sys.setdefaultencoding('utf-8')

class SogouNewSpider(Spider):
    name = "sogounew"
    domain_url = "http://news.sogou.com"
    start_urls = []

    def __init__ (self):
        super(SogouNewSpider,self).__init__()
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
        #timeTag = '&time=0'
        with open("keywords.txt","r") as inputs:
            for line in inputs:
                self.start_urls.append(self.domain_url + '/news?query=' + urllib.quote(line))
        
    #一个回调函数中返回多个Request以及Item的例子
    def parse(self,response):
        #print '====start %s==' %response.url
        #未成功获取query    
        if response.url == self.domain_url:
            print 'error of query'
            return
        self.log('a response from %s just arrived!' %response.url)
        #抽取并解析新闻网页内容
        items = self.parse_items(response)
        #构造一个Xpath的select对象，用来进行网页元素抽取
        sel = Selector(response)
        #抽取搜索结果页详细页面链接
        urls = sel.xpath(u'//div[@class="rb"]/h3/a/@href').extract()
        requests = []
        for url in urls:
            requests.append(self.make_requests_from_url(url).replace(callback=self.parse_content))
        
        for url in sel.xpath(u'//a[@class="np"]/@href').extract():
            requests.append(self.make_requests_from_url(self.domain_url + '/news' + url))
            
        for item in items:
            yield item
        #return requests
        for request in requests:
            yield request

    def parse_content(self,response):
        item = SogouNewsItem()
        item['url'] = response.url
        if response.body:
            bsoup = BeautifulSoup(response.body)
        item['content'] = bsoup.get_text()
        yield item

    def parse_items(self,response):
        if response.body:
            bsoup = BeautifulSoup(response.body)
        main_content = bsoup.select('div#wrapper')[0]
        
        if main_content:
            elem_list = main_content.find_all('div', class_='rb')
        items = []
        if len(elem_list) > 0:
            for elem in elem_list:
                item = SogouNewsItem()
                if elem.h3.a.get_text():
                    item['title'] = elem.h3.a.get_text()
                else:
                    continue
                item['url'] = elem.h3.a['href']
                author = elem.cite.get_text()
                if len(author.split()) > 1:
                    item['source'] = author.split()[0]
                    item['createTime'] = ' '.join(author.split()[1:])
                else:
                    item['source'] = author.split()[0] 
                    
                if elem.find('span',class_='sn_snip'):
                    item['abstract']=elem.find('div',class_='thumb_news').get_text()
                items.append(item)
            return items