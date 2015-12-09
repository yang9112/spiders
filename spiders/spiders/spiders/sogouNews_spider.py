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

class SogouNewSpider(Spider):
    name = "sogounew"
    domain_url = "http://news.sogou.com/news"
    start_urls = []
    tool = Utools()
    dc = dataCleaner()
    test_hbase = True

    def __init__ (self):
        super(SogouNewSpider,self).__init__()
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
        #sort_by_time = '&sort=1'
        sort_by_time = ''
        qlist = GetQuery().get_data()
        for query in qlist:
            if query:
                query_url = '?query=' + urllib.quote(query.encode('utf8')) + sort_by_time
                self.start_urls.append(self.domain_url + query_url)
        
    #一个回调函数中返回多个Request以及Item的例子
    def parse(self,response):
        # test the status of hbase and thrift server
        if self.test_hbase:
            try:
                self.htable=HBaseTest(host = self.tool.HOST_HBASE1, 
                                      table = 'origin')
                self.htable.close_trans()
                self.test_hbase = False
            except:
                raise CloseSpider('no thrift or hbase server!')        
        
        #print '====start %s==' %response.url
        #未成功获取query    
        if response.url == self.domain_url:
            print 'error of query'
            return
        
        #抽取并解析新闻网页内容
        items = self.parse_items(response)
        #构造一个Xpath的select对象，用来进行网页元素抽取
        sel = Selector(response)
        #抽取搜索结果页详细页面链接
        
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
        try:
            charset = response.encoding
        except:
            charset = 'utf-8'

        if response.body:
            try:
                bsoup = BeautifulSoup(response.body, from_encoding=charset)
                item['content'] = self.dc.process(str(bsoup).decode(charset))
            except:
                bsoup = BeautifulSoup(response.body, from_encoding='utf-8')
                item['content'] = self.dc.process(str(bsoup))
            if len(item['content'].encode('utf8')) < len(item['abstract']):
                item['content'] = item['abstract'].replace('百度快照', '')
            if item['content']:
                print 'url: ' + item['url'] + ' is added'
                return item

    def parse_items(self,response):
        if response.body:
            #去除干扰内容<!.*?>
            res = re.sub(r'<!.*?>', '', response.body)
            bsoup = BeautifulSoup(res, from_encoding='utf8')
        main_content = bsoup.select('div#wrapper')[0]
        if main_content:
            elem_list = main_content.find_all('div', class_='rb')
        items = []
        if len(elem_list) > 0:
            for elem in elem_list:
                item = DataItem()
                item['dtype'] = 'news'
                item['source'] = '搜狗新闻'
                item['channel'] = 'Search engine'
                if elem.h3.a.get_text():
                    item['title'] = elem.h3.a.get_text()
                else:
                    continue
                item['url'] = elem.h3.a['href']
                
                author = elem.cite.get_text()
                if len(author.split()) > 1:
                    item['medianame'] = author.split()[0]
                    item['pubtime'] = ' '.join(author.split()[1:])
                    if self.tool.old_news(item['pubtime']):
                        continue
                else:
                    item['source'] = author.split()[0]

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
                item['abstract']=elem.find('div',class_='ft').get_text()
                items.append(item)
        return items