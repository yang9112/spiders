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
from bs4 import BeautifulSoup
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

    def __init__ (self):
        super(SogouWeixinSpider,self).__init__()
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
        qlist = GetQuery().get_data()
        for query in qlist:
            if query:
                self.start_urls.append(self.domain_url + '?type=2&query=' + urllib.quote(query.encode('utf8')))
        
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

        requests = []
        for url in sel.xpath(u'//a[@class="np"]/@href').extract():
            requests.append(self.make_requests_from_url(self.domain_url + url))

        for item in items:
            yield Request(url=item['url'], meta={'item': item}, callback=self.parse_content)
            
        #return requests
        for request in requests:
            yield request

    def parse_content(self,response):
        item = response.meta['item']
        if response.body:
            bsoup = BeautifulSoup(response.body)
        item['content'] = bsoup.select('div#page-content')[0].get_text()
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
                
                item['collecttime'] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                item['abstract']=elem.p.get_text()
                items.append(item)
        return items