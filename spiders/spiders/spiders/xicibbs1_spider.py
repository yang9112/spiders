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
from redis import Redis
import json,re
import time
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

class XicibbsSpider(Spider):
    name = "xicibbs1"
    domain_url = "http://www.xici.net/"
    tool = Utools()
    dc = dataCleaner()
    start_urls = []
    xici_dict = dict()
    test_hbase = True
    
    def __init__ (self):
        super(XicibbsSpider,self).__init__()
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
        fp = open('xici.txt', 'rb')
        for line in fp.readlines():
            keys = line.split('\t');
            self.xici_dict.setdefault(keys[1], keys[0].decode('utf8'))
        fp.close()
        
        tag = '?sort=date'
        for key in self.xici_dict.keys():
            self.start_urls.append(key + tag)
            break;
            
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

        for item in items:
            yield Request(url=item['url'], meta={'item': item}, callback=self.parse_content)

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

    def parse_items(self, response):
        elem_list = []        
        items = []
        content = re.findall(r'"docinfo":\[.*?\]', response.body)
        source_name = self.xici_dict[response.url.split('?')[0]]
        print source_name
        
        
        if len(content) > 0:
            elem_list = re.findall('{.*?}', content[0])
        
        if len(elem_list) > 0:
            for elem in elem_list:
                item = DataItem()
                elem = json.loads(elem.decode('gb2312'))
                
                item['source'] = '西祠胡同'
                item['channel'] = 'Search engine'
                
                item['collecttime'] = time.strftime("%Y-%m-%d %H:%M", time.localtime())
                item['pubtime'] = item['collecttime'][0:4] + '-' + elem['ShortDate']
                if self.tool.old_news(item['pubtime']):
                    continue
                
                item['url'] = 'http://www.xici.net/d%s.htm' % elem['aDocs_i_0']
                item['title'] = elem['aDocs_i_1']
                
                items.append(item)
                
        return items