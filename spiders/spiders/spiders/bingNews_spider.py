#!/usr/bin/python
#-*-coding:utf-8-*-
from scrapy import Spider
from scrapy import Request
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.selector import Selector
from scrapy.exceptions import CloseSpider
from spiders.items import DataItem
from spiders.tools import Utools
from spiders.query import GetQuery
from spiders.dataCleaner import dataCleaner
from spiders.hbaseClient import HBaseTest
from bs4 import BeautifulSoup
from redis import Redis
import time
import json,re
import sys
import urllib

reload(sys)
sys.setdefaultencoding('utf-8')
sys.setrecursionlimit(5000)

class BingNewSpider(Spider):
    name = "bingnew"
    domain_url = "http://cn.bing.com"
    start_urls = []
    tool = Utools()
    dc = dataCleaner()
    test_hbase = True

    def __init__ (self):
        super(BingNewSpider,self).__init__()
        #将final绑定到爬虫结束的事件上
        dispatcher.connect(self.initial,signals.engine_started)
        dispatcher.connect(self.finalize,signals.engine_stopped)
        self.r = Redis(host = self.tool.HOST_REDIS1, port = 6379, db = 3)
    
    def initial(self):
        self.log('---started----')
        self.getStartUrl()
        #self.htable=HBaseTest(table = 'origin')

    def finalize(self):
        self.log('---stopped---')
        #self.htable.close_trans()
        #url持久化

    def getStartUrl(self):
        #从文件初始化查询关键词
        #sort_by_time = '&qft=sortbydate%3d"1"'
        sort_by_time = ''
        qlist = GetQuery().get_data()
        for query in qlist:
            if query:
                query_url = '/news/search?q=' + urllib.quote(query.encode('utf8')) + sort_by_time
                self.start_urls.append(self.domain_url + query_url)
    
    #一个回调函数中返回多个Request以及Item的例子
    def parse(self,response):
        # test the status of hbase and thrift server
        if self.test_hbase:
            try:
                self.htable=HBaseTest(table = 'origin')
                self.htable.close_trans()
                self.test_hbase = False
            except:
                raise CloseSpider('no thrift or hbase server!')
        
        #print '====start %s==' %response.url
        
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
            continue
            yield request

    def parse_content(self,response):
        item = response.meta['item']
        try:
            charset = response.charset
        except:
            charset = 'utf-8'

        try:
            for meta_item in response.xpath('//meta[@http-equiv]').extract():
                is_exsit = re.match('charset=(.*?)"', meta_item)
                if is_exsit:
                    charset = is_exsit.group(0)
                    break
        except:
            pass
        
        if response.body:
            try:
                bsoup = BeautifulSoup(response.body, from_encoding=charset)
            except:
                bsoup = BeautifulSoup(response.body, from_encoding='utf-8')
            item['content'] = self.dc.process(str(bsoup))
            if len(item['content'].encode('utf8')) < len(item['abstract']):
                item['content'] = item['abstract']
            if item['content']:
                print 'url: ' + item['url'] + ' is added'
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
                item = DataItem()
                item['dtype'] = 'news'
                item['source'] = '必应资讯'
                item['channel'] = 'Search engine'
                
                title = elem.find('div', 'newstitle')
                if title and title.a.get_text():
                    item['title'] = title.a.get_text()
                else:
                    continue
                item['url'] = title.a['href']
                
                author = elem.find('span',class_='sn_ST')
                if author:
                    #m = re.search('(\d{4}\/\d{1,2}\/\d{1,2})',source_time[0])
                    item['medianame'] = author.cite.get_text()
                    item['pubtime'] = self.normalize_time(str(author.span.get_text()))
                    if self.tool.old_news(item['pubtime']):
                        continue
                else:
                    print 'no element of author'
                    continue
                                
                if item['url'].find("html?") > 0 or item['url'].find("htm?") > 0:
                    item['url'] = "".join(item['url'].split("?")[0:-1])
                
                if self.r.exists(item['url']): 
                    #if self.htable.getRowByColumns(item['url'], ['indexData:url']):
                    continue
                
                try:                
                    item['source'] = self.tool.get_realname(item['medianame'])
                    item['medianame'] = ' '
                except:
                    pass
                
                item['collecttime'] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                if elem.find('span',class_='sn_snip'):
                    item['abstract']=elem.find('span',class_='sn_snip').get_text()
                items.append(item)
        return items
     
    def normalize_time(self, time_text):
        time_text = time_text.encode('utf8')
        if re.match('\d{4}.*?\d{1,2}.*?\d{1,2}', time_text):
            time_text = time_text.replace('/', '-') + ' 00:00'
        else:
            #非标准时间转换为时间戳,再转为标准时间
            time_digit = float(filter(str.isdigit, time_text))
            
            interval = 0;
            if time_text.find('天'.encode('utf8')) > 0 or time_text.find('day') > 0:
                interval = 86400
            elif time_text.find('时'.encode('utf8')) > 0 or time_text.find('hour') > 0:
                interval = 3600
            elif time_text.find('分'.encode('utf8')) > 0 or time_text.find('min') > 0:
                interval = 60
            elif time_text.find('秒'.encode('utf8')) > 0 or time_text.find('second') > 0:
                interval = 1
            else:
                return time_text
            
            time_true = time.time() - time_digit*interval
            time_text = time.strftime("%Y-%m-%d %H:%M", time.localtime(time_true))

        return time_text