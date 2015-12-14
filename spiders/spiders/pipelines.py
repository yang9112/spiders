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
import traceback

reload(sys)
sys.setdefaultencoding('utf-8')

mutex=threading.Lock()

class JsonWriterPipeline(object):
    def __init__(self):
        try:
            #self.file1=codecs.open('items.jl','a',encoding='utf-8')
            self.file2=codecs.open('content.jl','a',encoding='utf-8')
        except IOError,e:
            print 'file open error: ' + e.strerror

    def process_item(self, item, spider):
        line=json.dumps(dict(item),ensure_ascii=False)+"\n"
        if 'content' in item:
            self.file2.write(line)
#        else:
#            self.file1.write(line)
        return item
        
class UrlsPipeline(object):
    def __init__(self):
        self.items = []
        self.redis_timeout = False
        self.cachesize= 50
        self.expire_time = 3600*24*7
        self.tool = Utools()
        self.hbase_tag = True

        #事件绑定
        dispatcher.connect(self.initialize,signals.engine_started)
        dispatcher.connect(self.finalize,signals.engine_stopped)
        
    def initialize(self):
        #hbase
        try:
            self.htable=HBaseTest(table = 'origin')
        except:
            self.hbase_tag = False
            print 'can not connect to thrift host: ' + self.tool.HOST_HBASE
            
        self.htable1=HBaseTest(host = self.tool.HOST_HBASE1, table = 'origin')

        #redis
        try:
            self.redis_db3 = redis.Redis(host=self.tool.HOST_REDIS1, 
                                         port=6379, db=3, socket_timeout=1)
            self.redis_db0 = redis.Redis(host=self.tool.HOST_REDIS1,
                                         port=6379, db=0, socket_timeout=1)
        except Exception, e:
            print 'connect failed: ' + e.strerror
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
            print 'redis open error:' + e.strerror
            return

    def finalize(self):
        if not self.redis_timeout:
            if len(self.items) > 0:
                pipe=self.client.pipeline()
                for item in self.items:
                    try:
                        self.htable1.put_Item(item)
                    except:
                        print item
                        traceback.print_exc()                        
                        break
                    
                    if self.hbase_tag:
                        try:
                            self.htable.put_Item(item)
                        except:
                            traceback.print_exc()
                            print 'url: '+ item['url'] + ' saved failed'
                    
                    key = item['url'].encode('utf8')
                    self.redis_db3.set(key, key, self.expire_time)
                    pipe.rpush('linkbase', key)
                    self.redis_db0.rpush('linkbase', key)
                self.redis_timeout = False
                pipe.execute()
                
        if self.hbase_tag:
            self.htable.close_trans()
        self.htable1.close_trans()
            
    def writeToHbaseRedis(self):
        if mutex.acquire(1):
            pipe=self.client.pipeline()
            for item in self.items:
                try:
                    self.htable1.put_Item(item)
                except:
                    print item
                    traceback.print_exc()                    
                    break
                
                if self.hbase_tag:
                    try:
                        self.htable.put_Item(item)
                    except:
                        traceback.print_exc()
                        print 'url: '+ item['url'] + ' saved failed'
                
                key = item['url'].encode('utf8')
                self.redis_db3.set(key, key, self.expire_time)
                pipe.rpush('linkbase', key)
                self.redis_db0.rpush('linkbase', key)
                if self.redis_timeout == True:                            
                    self.redis_timeout = False
                        
            self.items=[]
            pipe.execute()
            mutex.release()

    def process_item(self, item, spider):
        if len(self.items) >= self.cachesize:
            self.writeToHbaseRedis()         
            
        if item.get('url','not_exists')!='not_exists':                    
            if not self.redis_timeout:
                key = item['url'].encode('utf8')
                try:               
                    if not self.redis_db3.exists(key):
                        self.items.append(item)
                except:
                    print "redis timeout error"
                    traceback.print_exc()
                    self.redis_timeout = True
            #if not self.client.sismember('crawled_set',item.get('url')):
                #self.client.sadd('crawled_set',item.get('url'))
        return item