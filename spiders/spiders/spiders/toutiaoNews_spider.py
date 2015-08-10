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
import json,re
import time
import sys
import urllib

reload(sys)
sys.setdefaultencoding('utf-8')
sys.setrecursionlimit(5000)

class BaiduNewSpider(Spider):
    name = "toutiaonew"
    domain_url = "http://toutiao.com/search_content"
    tool = Utools()
    dc = dataCleaner()
    start_urls = []
    test_hbase = True
    
    def __init__ (self):
        super(BaiduNewSpider,self).__init__()
        #将final绑定到爬虫结束的事件上
        dispatcher.connect(self.initial,signals.engine_started)
        dispatcher.connect(self.finalize,signals.engine_stopped)
    
    def initial(self):
        self.log('---started----')
        self.getStartUrl()
        self.r = Redis(host = self.tool.HOST_REDIS, port = 6379, db = 0)
            
    def finalize(self):
        self.log('---stopped---')
        #url持久化

    def getStartUrl(self):
        #从文件初始化查询关键词
        qlist = GetQuery().get_data()
        for query in qlist:
            if query:
                #默认时间排序
                query_url = "?offset=0&format=json&count=50&keyword=" + urllib.quote(query.encode('utf8'))
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
        
        #尝试寻找下一页
        requests = []
        try:
            url = sel.xpath(u'//p[@id="page"]/a[@class="n"]/@href').extract()[-1]
            requests.append(self.make_requests_from_url(self.domain_url+url))
        except:
            pass

        for item in items:
            yield Request(url=item['url'], meta={'item': item}, callback=self.parse_content)
        #return requests
        for request in requests:
            continue
            yield request

    def parse_content(self,response):
        item = response.meta['item']

        if item['url'].find('?') >= 0:
            item['url'] = response.url
            if self.r.sismember('crawled_set', item['url']):
                #if self.htable.getRowByColumns(item['url'], ['indexData:url']):
                return                         
        
        if response.body:
            bsoup = BeautifulSoup(response.body,from_encoding='utf-8')
            item['content'] = self.dc.process(str(bsoup))
            if item['content']:
                print 'url: ' + item['url'] + ' is added'
                return item

    def parse_items(self,response):
        if response.body:
            itemdatas = json.loads(response.body)['data']
        else:
            return []
        
        items = []
        for itemdata in itemdatas:
            item = DataItem()
            item['dtype'] = 'news'
            item['source'] = '今日头条'
            item['channel'] = 'Search engine'

            item['collecttime'] = time.strftime("%Y-%m-%d %H:%M", time.localtime())                        
            item['pubtime'] = itemdata['datetime']
            if self.tool.old_news(item['pubtime']):
                continue

            item['url'] = itemdata['display_url'].encode('utf8')
            if self.r.sismember('crawled_set', item['url']):
                #if self.htable.getRowByColumns(item['url'], ['indexData:url']):
                continue

            item['title'] = itemdata['title'].encode('utf8')
            item['medianame'] = itemdata['source'].encode('utf8')
            item['abstract'] = itemdata['abstract'].encode('utf8')
            items.append(item)
            
        return items