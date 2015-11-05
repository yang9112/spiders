# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import sys
import codecs
from tools import Utools
from hbaseClient import HBaseTest
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import redis
import threading

reload(sys)
sys.setdefaultencoding('utf-8')

mutex=threading.Lock()
class TestSpiderPipeline(object):
    def __init__(self):
        self.items=[]
        self.cachesize=50
        #事件绑定
        dispatcher.connect(self.initialize,signals.engine_started)
        dispatcher.connect(self.finalize,signals.engine_stopped)
    
    def writeToHbase(self):
        if mutex.acquire(1):
            for item in self.items:
                try:
                    self.htable.put_Item(item)
                    self.htable1.put_Item(item)
                except:
                    print 'url: '+ item['url'] + ' saved failed'
                    continue
            self.items=[]
            mutex.release()
        
    def process_item(self, item, spider):
        #向hbase写数据
        if len(self.items) >= self.cachesize:
            self.writeToHbase()
        if item.get('url','not_exists')!='not_exists':
            self.items.append(item)
        return item

    def initialize(self):
        self.htable=HBaseTest(table = 'origin')
        self.htable1=HBaseTest(host = '10.128.3.104', table = 'origin')
#        self.htable=HBaseTest(table = 'test')
		
    def finalize(self):
        if len(self.items) > 0:
            for item in self.items:
                try:
                    self.htable.put_Item(item)
                    self.htable1.put_Item(item)
                except:
                    print 'url: '+ item['url'] + ' saved failed'
                    continue
        self.htable.close_trans()
        self.htable1.close_trans()

class JsonWriterPipeline(object):
    def __init__(self):
        try:
            #self.file1=codecs.open('items.jl','a',encoding='utf-8')
            self.file2=codecs.open('content.jl','a',encoding='utf-8')
        except IOError,e:
            print 'file open error'

    def process_item(self, item, spider):
        line=json.dumps(dict(item),ensure_ascii=False)+"\n"
        if 'content' in item:
            self.file2.write(line)
#        else:
#            self.file1.write(line)
        return item
        
class UrlsPipeline(object):
    def __init__(self):
        self.urls=[]
        self.redis_timeout = False
        self.cachesize= 50
        self.expire_time = 3600*24*7
        try:
            self.redis_db3 = redis.Redis(host='10.128.3.116', port=6379, db=3, socket_timeout=1)
            self.redis_db0 = redis.Redis(host='10.128.3.116', port=6379, db=0, socket_timeout=1)
        except:
            print 'connect failed'
            pass
        
        try:
            try:
                self.host = Utools().HOST_REDIS
            except:
                print 'use the default host(redis):"localhost"'
                self.host = 'localhost'
            #self.pool = redis.ConnectionPool(host='10.133.5.48', port=6379, db=0)
            self.pool = redis.ConnectionPool(host=self.host, port=6379, db=0)
            self.client = redis.Redis(connection_pool=self.pool)
        except IOError,e:
            print 'redis open error'
            return
        dispatcher.connect(self.finalize,signals.engine_stopped)

    def finalize(self):
        if len(self.urls) > 0:
            pipe=self.client.pipeline()
            for url in self.urls:
                pipe.rpush('linkbase',url.encode('utf8'))
                self.redis_db0.rpush('linkbase', url.encode('utf8'))
            
            self.redis_timeout = False
            pipe.execute()

    def writeToRedis(self):
        if mutex.acquire(1):
            pipe=self.client.pipeline()

            for url in self.urls:
                pipe.rpush('linkbase',url.encode('utf8'))
                self.redis_db0.rpush('linkbase', url.encode('utf8'))
                if self.redis_timeout == True:                            
                    self.redis_timeout = False
            pipe.execute()
            self.urls=[]
            mutex.release()

    def process_item(self, item, spider):
        if len(self.urls)>=self.cachesize:
            self.writeToRedis()
        if item.get('url','not_exists')!='not_exists':
            url = item['url']                        
            key = url.encode('utf8')
            if not self.redis_timeout:
                try:               
                    if not self.redis_db3.exists(key):
                        self.redis_db3.set(key, key, self.expire_time)                             
                        self.urls.append(url)
                except:
                    print "redis timeout error"
                    self.redis_timeout = True                
            #if not self.client.sismember('crawled_set',item.get('url')):
                #self.client.sadd('crawled_set',item.get('url'))
        return item