# -*- coding: utf-8 -*-

#the definition of some Universal tool
import time

class Utools():
#    HOST_HBASE = 'localhost'
#    HOST_SQL = 'localhost'
#    HOST_REDIS = 'localhost'
    HOST_HBASE = '10.133.5.49'
    HOST_SQL = '10.133.5.48'
    HOST_REDIS = '10.133.5.48'
    
    def old_news(self, pubtime):
        try:
            old_time_value = time.mktime(time.strptime(pubtime, "%Y-%m-%d %H:%M"))
        except:
            print pubtime + ' time format illegal!\n'
            return True
        local_time_value = time.mktime(time.localtime())
        
        #只抓取24小时内的数据
        return (local_time_value - old_time_value) > 86400
        

if __name__ == '__main__':
    ut = Utools()
    print ut.old_news('2014-12-12 12:00')
    print ut.old_news('2015-12-12 11:10')
    print ut.old_news('2015-13-12 11:10')
    print ut.old_news('illegal')
    