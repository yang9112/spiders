# -*- coding: utf-8 -*-

from scrapy import Spider
from scrapy import Request
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.selector import Selector
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
#import urllib

reload(sys)
sys.setdefaultencoding('utf-8')
sys.setrecursionlimit(5000)

class BaiduNewSpider(Spider):
    name = "luchengbbs"
    domain_url = "http://www.zjxslm.com/"
    combine_url = 'forum.php?mod=forumdisplay&fid=%d&orderby=lastpost&filter=dateline&dateline=86400'
    
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
        ids = range(195, 209)
        ids.append(193)
        for idx in ids:
            self.start_urls.append("http://www.zjxslm.com/forum.php?"
                +"mod=forumdisplay&orderby=lastpost"
                +"&filter=dateline&dateline=86400&fid=%d" % idx)

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
        
        #抽取并解析新闻网页内容
        items = self.parse_items(response)
        
        #尝试寻找下一页
        requests = []
        if response.url.find('page') < 0:
            #构造一个Xpath的select对象，用来进行网页元素抽取
            sel = Selector(response)        
            page_num = sel.xpath('//div[@class="pg"]/label/span')

            if page_num:
                page_num = re.sub("<.*?>", "", page_num.extract()[0])
                page_num = int(re.search("([\d]+)", page_num).group(1))
                for idx in range(2, page_num+1):
                    url = response.url + ("&page=%d" % idx)
                    requests.append(self.make_requests_from_url(url))
                    
        for item in items:
            yield Request(url=item['url'], meta={'item': item}, callback=self.parse_content)
        #return requests
        for request in requests:
            yield request

    def parse_content(self,response):
        item = response.meta['item']
        
        if response.body:
            bsoup = BeautifulSoup(response.body, from_encoding='utf-8')
            item['pubtime'] = bsoup.find_all('div', class_="authi")[1].em.span['title']
            if self.tool.old_news(item['pubtime'][0:-3]):
                return
                
            item['content'] = str(bsoup.find('div', class_='pcb'))
            if item['content']:
                print 'url: ' + item['url'] + ' is added'
                return item

    def parse_items(self,response):
        if response.body:
            bsoup = BeautifulSoup(response.body,from_encoding='utf-8')
        main_content = bsoup.select('div#threadlist')[0]
        if main_content:
            elem_list = main_content.find_all('tbody')
        items = []
        
        if len(elem_list)>0:
            for elem in elem_list:
                item = DataItem()
                item['dtype'] = 'forum'
                item['source'] = '鹿城论坛'
                item['channel'] = 'Search engine'
                
                #抓取id获取url
                try:
                    tid = elem['id']
                except:
                    continue
                
                if tid.find('_') < 0:
                    continue
                else:
                    tid = tid.split('_')[1]
                
                item['url'] = self.domain_url + 'thread-' + tid + '-1-1.html'
                if self.r.exists(item['url']):
                    #if self.htable.getRowByColumns(item['url'], ['indexData:url']):
                    continue
                
                item['title'] = elem.find('th').get_text().split('\n')[2]
                item['medianame'] = elem.tr.find('td', class_='by').cite.get_text().replace('\n','')
                item['collecttime'] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                
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