#!/usr/bin/python
#-*-coding:utf-8-*-
from scrapy import Spider
from scrapy import Request
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.selector import Selector
from scrapy import log
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

class BaiduNewSpider(Spider):
    name = "tiebabbs"
    domain_url = "http://tieba.baidu.com"
    tool = Utools()    
    dc = dataCleaner()
    start_urls = []
    
    def __init__ (self):
        super(BaiduNewSpider,self).__init__()
        #将final绑定到爬虫结束的事件上
        dispatcher.connect(self.initial,signals.engine_started)
        dispatcher.connect(self.finalize,signals.engine_stopped)
    
    def initial(self):
        self.log('---started----')
        self.getStartUrl()
        self.r = Redis(host = self.tool.HOST_REDIS, port = 6379, db = 0)
        #self.htable=HBaseTest(table = 'origin')

    def finalize(self):
        self.log('---stopped---')
        #self.htable.close_trans()
        #url持久化

    def getStartUrl(self):
        #从文件初始化查询关键词
        qlist = GetQuery().get_data()
        for query in qlist:
            if query:
                #默认时间排序
                self.start_urls.append(self.domain_url+"/f/search/res?ie=utf-8&rn=20&qw="+urllib.quote(query.encode('utf8')) + '&ct=0')

    #一个回调函数中返回多个Request以及Item的例子
    def parse(self,response):
        #print '====start %s==' %response.url
        self.log('a response from %s just arrived!' %response.url)
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
        if response.body:
            bsoup = BeautifulSoup(response.body,from_encoding='utf-8')
            if bsoup.find('h1', class_='core_title_txt'):
                item['title'] = bsoup.find('h1', class_='core_title_txt')['title']
            elif bsoup.find('h3', class_='core_title_txt'):
                item['title'] = bsoup.find('h3', class_='core_title_txt')['title']
            else:
                return
            
            timeform = '%Y-%m-%d %H:%M'
            pubtimes = [time.strptime(item['pubtime'], timeform)]
            for pubtime in re.findall('/d{4}-/d{2}-/d{2} /d{2}:/d{2}', str(bsoup)):
                pubtimes.append(time.strptime(pubtime, timeform))
        
            item['pubtime'] = time.strftime(timeform, min(pubtimes))
            if self.tool.old_news(item['pubtime']):
                print item['utl'] + ' ' + item['pubtime']
                return           
            
            item['content'] = []
            for elem in bsoup.find_all('div', class_='d_post_content'):
                item['content'].append(str(elem.extract()))
                #onlt get the first floor
                break
            
            if item:
                item['content'] = ' '.join(item['content']).encode('utf8')
                item['content'] = self.dc.process(item['content'])
                print 'url: ' + item['url'] + ' is added'
                yield item

    def parse_items(self,response):
        if response.body:
            bsoup = BeautifulSoup(response.body,from_encoding='utf-8')
        main_content = bsoup.find('div', class_='s_post_list')
              
        items = []
        if main_content:
            elem_list = main_content.find_all('div', class_='s_post')
        else:
            return items
        
        if len(elem_list)>0:
            for elem in elem_list:
                item = DataItem()
                item['dtype'] = 'forum'
                item['source'] = '百度贴吧'
                item['channel'] = 'Search engine'
                try:
                    item['pubtime'] = elem.find('font', class_='p_date').get_text()
                    if self.tool.old_news(item['pubtime']):
                        continue
                    
                    #item['title'] = elem.span.a.get_text()
                    item['medianame'] = elem.find('font', class_='p_violet').get_text()
                    item['abstract'] = elem.find('div',class_='p_content').get_text()           
                except:
                    continue
                
                item['url'] = self.domain_url + re.findall('(/p/.*?)[^\d]', elem.span.a['href'])[0]
                if self.r.sismember('crawled_set', item['url']): 
                    #if self.htable.getRowByColumns(item['url'], ['indexData:url']):
                    continue
                
                item['collecttime'] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                items.append(item)
        #去重
        new_items = []
        url_list = []
        for item in items:
            if item['url'] not in url_list:
                new_items.append(item)
                url_list.append(item['url'])
        items = new_items;
        return items