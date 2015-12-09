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

class TianyaBBSSpider(Spider):
    name = "tianyabbs"
    domain_url = "http://search.tianya.cn/"
    tool = Utools()    
    dc = dataCleaner()
    start_urls = []
    test_hbase = True   
    
    def __init__ (self):
        super(TianyaBBSSpider,self).__init__()
        #将final绑定到爬虫结束的事件上
        dispatcher.connect(self.initial,signals.engine_started)
        dispatcher.connect(self.finalize,signals.engine_stopped)
    
    def initial(self):
        self.log('---started----')
        self.getStartUrl()
        self.r = Redis(host = self.tool.HOST_REDIS1, port = 6379, db = 3)        
        #self.htable=HBaseTest(table = 'origin')

    def finalize(self):
        self.log('---stopped---')
        #self.htable.close_trans()
        #url持久化

    def getStartUrl(self):
        #从文件初始化查询关键词
        #发帖时间
        pageTag = '&s=4'
        #回复时间
        #pageTag = '&s=6'   
        #默认相关性排序       
        
        qlist = GetQuery().get_data()
        for query in qlist:
            if query:
                query_url = '/bbs?q=' + urllib.quote(query.encode('utf8')) + pageTag
                self.start_urls.append(self.domain_url + query_url)
        
    #一个回调函数中返回多个Request以及Item的例子
    def parse(self,response):
        #print '====start %s==' %response.url
        # test the status of hbase and thrift server
        if self.test_hbase:
            try:
                self.htable=HBaseTest(host = self.tool.HOST_HBASE1, 
                                      table = 'origin')
                self.htable.close_trans()
                self.test_hbase = False
            except:
                raise CloseSpider('no thrift or hbase server!')
        
        #抽取并解析新闻网页内容
        items = self.parse_items(response)
        #构造一个Xpath的select对象，用来进行网页元素抽取
        sel = Selector(response)
        #抽取搜索结果页详细页面链接

        requests = []
        for url in sel.xpath(u'//div[@class="long-pages"]/a[text()="下一页"]/@href').re('go\(([\d]*?)\)'):
            tp_url = re.sub('&pn=[\d]+?', '', response.url)
            requests.append(self.make_requests_from_url(tp_url + '&pn=' + url))

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
            
            item_content_list = bsoup.find_all('div', class_='bbs-content')
            
            #only get the first floor
            if len(item_content_list) > 0:
                item['content'] = item_content_list[0].extract().encode('utf8')
                #item['content'] = ' '.join(v.get_text().encode('utf8') for v in item_content_list)
            item['content'] = re.sub(r'\n|\t|\r', '', item['content'])
            item['content'] = self.dc.process(item['content'])
            if item['content']:
                print 'url: ' + item['url'] + ' is added' 
                return item

    def parse_items(self,response):
        if response.body:
            bsoup = BeautifulSoup(response.body)
        main_content = bsoup.select('div#main')[0]
        #查询项中有一项多余
        if main_content:
            if main_content.select('li#search_msg'):
                elem_list = main_content.find_all('li')[:-1]
            else:
                elem_list = main_content.find_all('li')
                
        items = []
        if len(elem_list) > 0:
            for elem in elem_list:
                item = DataItem()
                item['dtype'] = 'forum'
                item['source'] = '天涯论坛'
                item['channel'] = 'Search engine'
                try:
                    item['title'] = elem.div.h3.a.get_text()
                except:
                    continue
                item['url'] = elem.div.h3.a['href']         
                
                author = elem.find('p', class_='source')
                if author:
                    item['medianame'] = author.a.get_text()
                    #item['author'] = author.a.get_text()                    
                    if author.span.get_text().find('-') > 0:
                        item['pubtime'] = author.span.get_text()
                    else:
                        item['pubtime'] = author.find_all('span')[-2].get_text()
                    if self.tool.old_news(item['pubtime']):
                        continue
                else:
                    print 'element of author no found!\n'
                    return

                if self.r.exists(item['url']):  
                    #if self.htable.getRowByColumns(item['url'], ['indexData:url']):
                    continue
                
                item['collecttime'] = time.strftime("%Y-%m-%d %H:%M", time.localtime())                
                item['abstract']=elem.div.p.get_text()
                items.append(item)
        return items