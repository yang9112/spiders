#!/usr/bin/python
#-*-coding:utf-8-*-
from scrapy import Spider
from scrapy import Request
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.selector import Selector
from scrapy import log
from spiders.items import BaiduNewsItem
from spiders.tools import Utools
from spiders.query import GetQuery
from bs4 import BeautifulSoup
from redis import Redis
import json,re
import redis
import time
import sys
import urllib

reload(sys)
sys.setdefaultencoding('utf-8')

class BaiduNewSpider(Spider):
    name = "baidunew"
    domain_url = "http://news.baidu.com"
    tool = Utools()
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

    def finalize(self):
        self.log('---stopped---')
        #url持久化

    def getStartUrl(self):
        #从文件初始化查询关键词
        qlist = GetQuery().get_data()
        for query in qlist:
            if query:
                #默认时间排序
                self.start_urls.append(self.domain_url+"/ns?rn=20&word="+urllib.quote(query.encode('utf8')) + '&ct=0')

    #一个回调函数中返回多个Request以及Item的例子
    def parse(self,response):
        print '====start %s==' %response.url
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
        item['content'] = bsoup.get_text()
        yield item

    def parse_items(self,response):
        if response.body:
            bsoup = BeautifulSoup(response.body,from_encoding='utf-8')
        main_content = bsoup.select('div#container')[0].select('div#content_left')[0]
       
        if main_content:
            elem_list = main_content.select("ul > li")
        items = []
        if len(elem_list)>0:
            for elem in elem_list:
                item = BaiduNewsItem()
                item['type'] = 'news'
                item['source'] = '百度新闻'
                try:
                    item['title'] = elem.h3.a.get_text()
                except:
                    continue
                item['url'] = elem.h3.a['href']
                
                author = elem.find('p',class_='c-author')
                if author:
                    source_time = author.get_text().split()
                    if re.match(r'\d{4}.*?\d{1,2}.*?\d{1,2}', source_time[0]):
                        item['medianame'] = 'None'
                        item['pubtime'] = self.normalize_time(str(' '.join(source_time)))
                    else:
                        item['medianame'] = source_time[0]
                        item['pubtime'] = self.normalize_time(str(' '.join(source_time[1:])))
                        
                    if self.tool.old_news(item['pubtime']):
                        continue                        
                else:
                    print 'no element of author'
                    continue

                if self.r.sismember('crawled_set', item['url']):  
                    continue
                
                print 'url: ' + item['url'] + 'is added'
                item['collecttime'] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                if elem.find('div',class_='c-summary'):
                    item['abstract'] = elem.find('div',class_='c-summary').get_text()
                items.append(item)
        return items
                
    def normalize_time(self, time_text):
        if re.match('\d{4}.*?\d{1,2}.*?\d{1,2}.*?\d{1,2}:\d{1,2}', time_text):
            time_text = time_text.replace('年', '-').replace('月', '-').replace('日', '')
        else:
            #非标准时间转换为时间戳,再转为标准时间
            time_digit = float(filter(str.isdigit, time_text))
            
            interval = 0;
            if time_text.find('天') > 0:
                interval = 86400
            elif time_text.find('时') > 0:
                interval = 3600
            elif time_text.find('分') > 0:
                interval = 60
            elif time_text.find('秒') > 0:
                interval = 1
            else:
                return time_text
            
            time_true = time.time() - time_digit*interval
            time_text = time.strftime("%Y-%m-%d %H:%M", time.localtime(time_true))

        return time_text