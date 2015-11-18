#!/usr/bin/python
#-*-coding:utf-8-*-
from scrapy import Spider
from scrapy import Request
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.exceptions import CloseSpider
from spiders.items import DataItem
from spiders.tools import Utools
from spiders.dataCleaner import dataCleaner
from spiders.hbaseClient import HBaseTest
from bs4 import BeautifulSoup
from redis import Redis
import json,re
import time
import sys
import requests

reload(sys)
sys.setdefaultencoding('utf-8')

class B2bNewSpider(Spider):
    name = "b2bnew"
    domain_url = "http://b2b.10086.cn/"
    tool = Utools()
    dc = dataCleaner()
    start_urls = []
    test_hbase = True
    
    def __init__ (self):
        super(B2bNewSpider,self).__init__()
        #将final绑定到爬虫结束的事件上
        dispatcher.connect(self.initial,signals.engine_started)
        dispatcher.connect(self.finalize,signals.engine_stopped)
    
    def initial(self):
        self.log('---started----')
        self.getStartUrl()
        self.r = Redis(host = self.tool.HOST_REDIS1, port = 6379, db = 3)
    
    def finalize(self):
        self.log('---stopped---')
        #url持久化

    def getStartUrl(self):
        #从文件初始化查询关键词
        url = 'http://b2b.10086.cn/b2b/main/listVendorNotice.html?noticeType=2'        
        self.start_urls.append(url)
        
    #一个回调函数中返回多个Request以及Item的例子
    def parse(self, response):
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

        for item in items:
            yield Request(url=item['url'], meta={'item': item}, callback=self.parse_content)

    def parse_content(self,response):
        item = response.meta['item']
        
        if response.body:
            bsoup = BeautifulSoup(response.body, from_encoding='utf-8')
    
            content = bsoup.select('div#mobanDiv')[0]
            print content
        else:
            return

    def parse_items(self, response):
        elem_list = []        
        items = []
        url = "http://b2b.10086.cn/b2b/main/listVendorNoticeResult.html?noticeBean.noticeType=2"
        data = "&page.currentPage=1&page.perPageSize=50&noticeBean.sourceCH=&noticeBean.source=&noticeBean.title=&noticeBean.startDate=&noticeBean.endDate="
        elem_list = re.findall('<tr(.*?)</tr>', re.sub('\s', '', requests.post(url + data).text))
        
        if len(elem_list) > 0:
            for elem in elem_list:
                item = DataItem()
                
                item['source'] = '中国移动采购与招标'
                item['channel'] = 'Search engine'                
                
                if elem.find("onclick") < 0:
                    continue
                itemID = re.search("selectResult\(\'([\d]+?)\'\)", elem).group(1)                                
                item['url'] = ('http://b2b.10086.cn/b2b/main/viewNoticeContent.html?'
                    + 'noticeBean.id=' + itemID)
                
                if self.r.exists(item['url']):
                    continue
                
                res = re.findall('<td.*?<\td>', elem)
                item['medianame'] = re.sub('<.*?>', res[0])
                item['title'] = re.sub('<.*?>', '', res[2])
                
                item['collecttime'] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                item['pubtime'] = re.sub('<.*?>', '', res[-1]) + item['collecttime'][-6:]
                if self.tool.old_news(item['pubtime']):
                    continue
                
                items.append(item)
                
        return items