#!/usr/bin/python
#-*-coding:utf-8-*-
from scrapy import Spider
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.selector import Selector
from scrapy import log
from spiders.items import BaiduNewsItem 
from bs4 import BeautifulSoup
import json,re
import sys
import urllib

reload(sys)
sys.setdefaultencoding('utf-8')

class BaiduNewSpider(Spider):
    name = "baidunew"
    domain_url = "http://news.baidu.com"
    start_urls = []
    
    def __init__ (self):
        super(BaiduNewSpider,self).__init__()
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
        with open("keywords.txt","r") as inputs:
            for line in inputs:
                self.start_urls.append(self.domain_url+"/ns?rn=20&word="+urllib.quote(line))

    #一个回调函数中返回多个Request以及Item的例子
    def parse(self,response):
        #print '====start %s==' %response.url
        self.log('a response from %s just arrived!' %response.url)
        #抽取并解析新闻网页内容
        items=self.parse_items(response)
        #构造一个Xpath的select对象，用来进行网页元素抽取
        sel=Selector(response)
        #抽取搜索结果页详细页面链接
        urls=sel.xpath(u'//ul/li/h3[@class="c-title"]/a/@href').extract()
        requests=[]
        for url in urls:
            requests.append(self.make_requests_from_url(url).replace(callback=self.parse_content))
#        #此处添加有问题
#        for url in sel.xpath(u'//*[@id="page"]/a[text()="下一页>"]/@href').extract():
#            requests.append(self.make_requests_from_url(self.domain_url+url))
        for item in items:
            yield item
        #return requests
        for request in requests:
            yield request

    def parse_content(self,response):
        item=BaiduNewsItem()
        item['url']=response.url
        if response.body:
            bsoup=BeautifulSoup(response.body,from_encoding='utf-8')
        item['content']=bsoup.get_text()
        yield item

    def parse_items(self,response):
        if response.body:
            bsoup=BeautifulSoup(response.body,from_encoding='utf-8')
        main_content=bsoup.select('div#container')[0].select('div#content_left')[0]
        if main_content:
            elem_list=main_content.select("ul > li")
        items=[]
        if len(elem_list)>0:
            for elem in elem_list:
                item=BaiduNewsItem()
                if elem.h3.a.get_text():
                    item['title']=elem.h3.a.get_text()
                else:
                    continue
                item['url']=elem.h3.a['href']
                author=elem.find('p',class_='c-author')
                if author:
                    source_time=author.get_text().split()
                    if len(source_time)>1:
                        m=re.search('(\d{4}-\d{2}-\d{2})',source_time[0])
                        if m:
                            item['createTime']=author.get_text()
                        else:
                            item['source']=source_time[0]
                            item['createTime']=' '.join(source_time[1:])
                if elem.find('div',class_='c-summary'):
                    item['abstract']=elem.find('div',class_='c-summary').get_text()
                items.append(item)
            return items