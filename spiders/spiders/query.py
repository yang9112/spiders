# -*- coding: utf-8 -*-

import MySQLdb
import sys
from tools import Utools

reload(sys)
sys.setdefaultencoding('utf-8')

class GetQuery():
    QueryNum = -1

    def __init__(self,
                host= 'localhost',
                port = 3306,
                user='scrm',
                passwd='123456',
                db ='scrm',
                tablename = 'subscribeinfo',
                charset = 'utf8'):
        
        try:
            self.host = Utools().HOST_SQL
        except:
            print 'use the default host of sql:"localhost"'
            self.host = host
            
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.tablename = tablename
        self.charset = charset

    def get_data(self):        
        conn= MySQLdb.connect(
                host = self.host,
                port = self.port,
                user= self.user,
                passwd = self.passwd,
                db = self.db,
                charset = self.charset
                )                    
        cur = conn.cursor()
#        cur.execute("create table query(name varchar(50))")
        #get data from mysql
        info = cur.fetchmany(cur.execute("select titlekeywords, contentkeywords from %s" % self.tablename))        
        info_list = []
        #change the data form(tuple) to list
        for name in info:
            info_list.extend(list(name))
        
        try:
            if self.QueryNum == -1:
                info_list = list(set(self.split_query(info_list)))
            else:
                info_list = list(set(self.split_query(info_list)[0:self.QueryNum]))
        except:
            info_list = list(set(self.split_query(info_list)))
            
        cur.close()
        conn.commit()
        conn.close()        
        return info_list
    
    def split_query(self, querylist):
        info_list = []
#        for query in querylist:
#            if query:
#                query = query.replace('(', '').replace(')', '')
#                #nedd to combine the key words or not?
#                newquery_list = query.split('+')
#                if len(newquery_list) > 1:
#                    for key1 in newquery_list[0].split('|'):
#                        for key2 in '|'.join(newquery_list[1:]).split('|'):
#                            if key1 and key2:                           
#                                info_list.append(key1 + ' ' + key2)
#                else:
#                    newquery_list[0].replace(u'\、'.encode('utf8'), '|')
#                    info_list.extend(newquery_list[0].split('|'))
        
        for query in querylist:
            if query:
                query = query.replace('(', '').replace(')', '').replace('+', '|').replace('、'.encode('utf8'), '|')
                #nedd to combine the key words or not?
                info_list.extend(query.split('|'))
        
        return info_list
        
if __name__ == '__main__':
    mysql = GetQuery()    
    #mysql = GetQuery(host= '10.133.5.48', port = 3306, user='scrm',
    #                 passwd='123456', db ='scrm', tablename = 'subscribeinfo')
#    mysql = GetQuery(host='localhost', port = 3306, user='root',
#                 passwd='123zxc', db ='spider_db', tablename = 'query')
    info_list = mysql.get_data()
    for name in info_list:
        print name
#    ff = open('s.log','wb');
#    for name in info_list:
#        ff.writelines(name.encode('utf8') + '\n')
#    ff.close()
    print len(info_list)