#!/usr/bin/python
#-*-coding:utf-8-*-
from scrapy import Spider
from scrapy import Request
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.selector import Selector
from scrapy import log
from spiders.items import TianyaBBSItem
from spiders.query import GetQuery
from bs4 import BeautifulSoup
import json,re
import sys
import urllib

reload(sys)
sys.setdefaultencoding('utf-8')

class TianyaBBSSpider(Spider):
    name = "tianyabbs"
    domain_url = "http://search.tianya.cn/"
    start_urls = []

    def __init__ (self):
        super(TianyaBBSSpider,self).__init__()
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
        #发帖时间
        pageTag = '&s=4'
        #回复时间
        #pageTag = '&s=6'   
        #默认相关性排序       
        
        qlist = GetQuery().get_data()
#        qlist = ['中国电信']
        for query in qlist:
            new_url = self.domain_url + '/bbs?q=' + urllib.quote(query.encode('utf8')) + pageTag
            self.start_urls.append(new_url)
        
    #一个回调函数中返回多个Request以及Item的例子
    def parse(self,response):
        #print '====start %s==' %response.url
        self.log('a response from %s just arrived!' %response.url)
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
            yield request

    def parse_content(self,response):
        item = response.meta['item']
        if response.body:
            bsoup = BeautifulSoup(response.body)
#        from scrapy.shell import inspect_response
#        inspect_response(response, self)
        item['content'] = ''
        item_content_list = bsoup.find_all('div', class_='atl-content')
        for item_content in item_content_list:
            item['content'] = item['content'] + re.sub(r'\n|\t|\r', '', item_content.get_text())
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
                item = TianyaBBSItem()
                if elem.div.h3.a.get_text():
                    item['title'] = elem.div.h3.a.get_text()
                else:
                    continue
                item['url'] = elem.div.h3.a['href']
                author = elem.find('p', class_='source')
                if author:
                    item['source'] = author.a.get_text()
                    item['author'] = author.a.get_text()                    
                    item['createTime'] = author.span.get_text()
                item['abstract']=elem.div.p.get_text()
                items.append(item)
            return items