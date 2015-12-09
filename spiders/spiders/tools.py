# -*- coding: utf-8 -*-

#the definition of some Universal tool
import time

class Utools():
    HOST_HBASE = 'localhost'
    HOST_SQL = 'localhost'
    HOST_REDIS = 'localhost'
    HOST_HBASE = '10.133.5.49'
    HOST_HBASE1 = '10.128.4.18'
    HOST_SQL = '10.128.3.114'
    HOST_REDIS = '10.133.5.48'
    HOST_REDIS1 = "10.128.3.116"

    #second
    time_interval = 3600*24
    
    def old_news(self, pubtime):
        try:
            old_time_value = time.mktime(time.strptime(pubtime, "%Y-%m-%d %H:%M"))
        except:
            print pubtime + ' time format illegal!\n'
            return True
        local_time_value = time.mktime(time.localtime())
        
        #只抓取24小时内的数据
        return (local_time_value - old_time_value) > self.time_interval
    
    def set_timeinterval(self, new_interval):
        self.time_interval = new_interval
    
    def get_realname(self, string_name):
        namedict = {
            'news.cz001.com.cn':'中国常州网',
            'nj.house.ifeng.com':'凤凰房产',
        }
        
        if string_name in namedict:
            return namedict[string_name]
            
        return string_name

if __name__ == '__main__':
    ut = Utools()
    print ut.old_news('2014-12-12 12:00')
    print ut.old_news('2015-12-12 11:10')
    print ut.old_news('2015-13-12 11:10')
    print ut.old_news('illegal')
    
