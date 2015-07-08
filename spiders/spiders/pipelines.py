# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json
import sys
import codecs
#from hbaseclient import HBaseTest
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
#from redis import Redis

reload(sys)
sys.setdefaultencoding('utf-8')

#==============================================================
# something about Hbase, I will do it later
#==============================================================
class TestSpiderPipeline(object):
	#def __init__(self):
		#事件绑定
	#	dispatcher.connect(self.initialize,signals.engine_started)
	#	dispatcher.connect(self.finalize,signals.engine_stopped)
	
	def process_item(self, item, spider):
		#向hbase写数据
		#if not item.get('title','not set')=='not set':
		#	print json.dumps(dict(item),ensure_ascii=False)
		#self.htable.put_Item(item)
		return item

#	def initialize(self):
#		self.htable=HBaseTest('news')
#		
#	def finalize(self):
#		self.htable.close_trans()

class JsonWriterPipeline(object):
    def __init__(self):
        try:
            self.file1=codecs.open('items.jl','a',encoding='utf-8')
            self.file2=codecs.open('content.jl','a',encoding='utf-8')
        except IOError,e:
            print 'file open error'

    def process_item(self, item, spider):
        line=json.dumps(dict(item),ensure_ascii=False)+"\n"
        if 'content' in item:
            self.file2.write(line)
        else:
            self.file1.write(line)
        return item