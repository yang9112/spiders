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

class SogouWeixinSpider(Spider):
    name = "sogouwx"
    domain_url = "http://weixin.sogou.com/weixin"
    start_urls = []
    tool = Utools()
    dc = dataCleaner()
    test_hbase = True

    def __init__ (self):
        super(SogouWeixinSpider,self).__init__()
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
        #过去24小时
        timeTag = '&tsn=1'
        qlist = GetQuery().get_data()
        
        for i in range(5):
            for query in qlist:
                if query:
                    query_url = '?type=2&query=' + urllib.quote(query.encode('utf8')) + timeTag
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
        
        print '====start %s==' %response.url        
        time.sleep(4)
        from scrapy.shell import inspect_response
        inspect_response(response, self)
        #未成功获取query    
        if response.url == self.domain_url:
            print 'error of query'
            return
        
        self.log('a response from %s just arrived!' %response.url)
        #抽取并解析新闻网页内容
        items = self.parse_items(response)
        #构造一个Xpath的select对象，用来进行网页元素抽取
        sel = Selector(response)

        requests = []
        for url in sel.xpath(u'//a[@class="np"]/@href').extract():
            requests.append(self.make_requests_from_url(self.domain_url + url))

        for item in items:
            yield Request(url=item['url'], meta={'item': item}, callback=self.parse_content)
            
        #return requests
        for request in requests:
            continue
            yield request

    def parse_content(self,response):
        item = response.meta['item']
        if response.body:
            bsoup = BeautifulSoup(response.body)
            
        print 'url:' + item['url'] + ' is added'
        item['content'] = str(bsoup.select('div#page-content')[0]).encode('utf8')
        return item

    def parse_items(self,response):
        if response.body:
            #去除干扰内容<!.*?>
            res = re.sub(r'<!.*?>', '', response.body)
            bsoup = BeautifulSoup(res, from_encoding='utf8')
        main_content = bsoup.select('div#wrapper')[0]
        
        if main_content:
            elem_list = main_content.find_all('div', class_='txt-box')
        items = []
        if len(elem_list) > 0:
            for elem in elem_list:
                item = DataItem()
                item['dtype'] = 'weixin'
                item['source'] = '搜狗微信'
                item['channel'] = 'Search engine'
                if elem.h4.a.get_text():
                    item['title'] = elem.h4.a.get_text()
                else:
                    continue
                item['url'] = elem.h4.a['href']
                item['medianame'] = elem.div.a['title']
                #时间戳转换时间
                item['pubtime'] = time.strftime('%Y-%m-%d %H:%M', time.localtime(float(elem.div['t'])))
                if self.tool.old_news(item['pubtime']):
                        continue
                if self.r.sismember('crawled_set', item['url']):
                    #if self.htable.getRowByColumns(item['url'], ['indexData:url']):
                    continue
                
                item['collecttime'] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                item['abstract']=elem.p.get_text()
                items.append(item)
        return items