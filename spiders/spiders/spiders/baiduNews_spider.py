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
    name = "baidunew"
    domain_url = "http://news.baidu.com"
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
        self.r = Redis(host = self.tool.HOST_REDIS1, port = 6379, db = 3)
            
    def finalize(self):
        self.log('---stopped---')
        #url持久化

    def getStartUrl(self):
        #从文件初始化查询关键词
        qlist = GetQuery().get_data()
        for query in qlist:
            if query:
                #默认时间排序
                query_url = "/ns?rn=20&word=" + urllib.quote(query.encode('utf8')) + '&ct=0'
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
        charset = 'utf-8'
        try:
            for meta_item in response.xpath('//meta[@http-equiv]').extract():
                is_exsit = re.match('charset=(.*?)"', meta_item)
                if is_exsit:
                    charset = is_exsit.group(0)
                    break
        except:
            pass
        
        if response.body:
            try:
                bsoup = BeautifulSoup(response.body, from_encoding=charset)
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
            bsoup = BeautifulSoup(response.body,from_encoding='utf-8')
        main_content = bsoup.select('div#container')[0].select('div#content_left')[0]
        if main_content:
            elem_list = main_content.find_all('div', class_='result')
        items = []
        
        if len(elem_list)>0:
            for elem in elem_list:
                item = DataItem()
                item['dtype'] = 'news'
                item['source'] = '百度新闻'
                item['channel'] = 'Search engine'
                try:
                    item['title'] = elem.h3.a.get_text()
                except:
                    continue
                item['url'] = elem.h3.a['href']

                author = elem.find('p',class_='c-author')
                if author:
                    source_time = author.get_text().split()
                    if re.match(r'\d{4}.*?\d{1,2}.*?\d{1,2}', source_time[0].encode('utf8')):
                        item['medianame'] = 'None'
                        item['pubtime'] = self.normalize_time(str(' '.join(source_time)))
                    elif filter(str.isdigit, source_time[0].encode('utf8')) and len(source_time) == 1:
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
                if elem.find('div',class_='c-summary'):
                    item['abstract'] = elem.find('div',class_='c-summary').get_text()
                items.append(item)
        return items
                
    def normalize_time(self, time_text):
        time_text = time_text.encode('utf8')
        if re.match('\d{4}.*?\d{1,2}.*?\d{1,2}.*?\d{1,2}:\d{1,2}', time_text):
            time_text = time_text.replace('年'.encode('utf8'), '-').replace('月'.encode('utf8'), '-').replace('日'.encode('utf8'), '')
        else:
            #非标准时间转换为时间戳,再转为标准时间
            time_digit = float(filter(str.isdigit, time_text))
            
            interval = 0;
            if time_text.find('天'.encode('utf8')) > 0:
                interval = 86400
            elif time_text.find('时'.encode('utf8')) > 0:
                interval = 3600.
            elif time_text.find('分'.encode('utf8')) > 0:
                interval = 60
            elif time_text.find('秒'.encode('utf8')) > 0:
                interval = 1
            else:
                return time_text
            
            time_true = time.time() - time_digit*interval
            time_text = time.strftime("%Y-%m-%d %H:%M", time.localtime(time_true))

        return time_text