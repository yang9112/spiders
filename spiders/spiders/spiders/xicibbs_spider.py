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

class XiciBBSSpider(Spider):
    name = "xicibbs"
    domain_url = "http://baidu.xici.net/cse"
    tool = Utools()
    dc = dataCleaner()
    start_urls = []
    test_hbase = True
    
    def __init__ (self):
        super(XiciBBSSpider,self).__init__()
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
        tag = '&s=11800334043319024933&srt=lds&sti=1440&nsid=0'
        qlist = GetQuery().get_data()
        for query in qlist:
            if query:
                #默认时间排序
                query_url = "/search?q="+urllib.quote(query.encode('utf8')) + tag
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
    
        main_content = response.xpath('//head').extract()[0]
        content_list = re.findall('({"del_w".*?})', main_content)
        
        if len(content_list) > 0:
            try:
                #store the keys
                content_list[0] = re.sub('<div.*?>', '<p>', content_list[0]).replace('</div>', '</p>')
                tags = re.findall('<.*?>', content_list[0].encode('utf8'))
                tagdict = dict()
                for i in range(len(tags)):
                    tagdict.setdefault('&tag_' + str(i) + ';', tags[i])
                
                for key in tagdict.keys():
                    content_list[0] = content_list[0].replace(tagdict[key], key)
                    tagdict[key] = str(tagdict[key].replace('\\"', ''))

                content_list[0] = content_list[0].replace('{','').replace('}', '')
                maindict = json.loads('{' + content_list[0] + '}', encoding='utf8')
                item['medianame'] = maindict['UserName']
                item['pubtime'] = maindict['really_updated_at'][:-3]
                if self.tool.old_news(item['pubtime']):
                    return
                
                item['content'] = []    
                for content in content_list:
                    content = re.sub('<.*?>', '', content).replace('{','').replace('}', '')
                    content_dict = json.loads('{' + content + '}', encoding='utf8')
                    if content_dict.has_key('floorcontent'):
                        #release the tags
                        for key in tagdict.keys():
                            content_dict['floorcontent'] = content_dict['floorcontent'].replace(key, tagdict[key])

                        content_dict['floorcontent'] = content_dict['floorcontent']                         
                        item['content'].append(content_dict['floorcontent'])
                        #only get the first floor                        
                        break
                if item:
                    item['content'] = self.dc.process('<div>' + ' '.join(item['content']) + '</div>')
                    print 'url: ' + item['url'] + ' is added'
                    return item
            except:
                print item['url'] + ' load failed.'
                pass
        else:
            return

    def parse_items(self,response):
        if response.body:
            bsoup = BeautifulSoup(response.body,from_encoding='utf-8')
        main_content = bsoup.select('div#results')[0]
        if main_content:
            elem_list = main_content.find_all('div', class_='result')
        items = []
        
        if len(elem_list)>0:
            for elem in elem_list:
                item = DataItem()
                item['dtype'] = 'forum'
                item['source'] = '西祠胡同'
                item['channel'] = 'Search engine'
                try:
                    item['title'] = elem.h3.a.get_text()
                except:
                    continue
                item['url'] = elem.h3.a['href'].replace('user', 'www')

                if self.r.exists(item['url']):
                    #if self.htable.getRowByColumns(item['url'], ['indexData:url']):
                    continue
                
                item['collecttime'] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                if elem.find('div',class_='c-summary'):
                    item['abstract'] = elem.find('div',class_='c-content').get_text()
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