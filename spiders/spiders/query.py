# -*- coding: utf-8 -*-

import MySQLdb
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

class GetQuery():

    def __init__(self, 
                host='localhost',
                port = 3306,
                user='root',
                passwd='123zxc',
                db ='spider_db',
                charset = 'utf8'):                    
       self.host = host
       self.port = port
       self.user = user
       self.passwd = passwd
       self.db = db
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
        info = cur.fetchmany(cur.execute("select * from query"));
        info_list = []
        #change the data form(tuple) to list
        for name in info:
            info_list.append(list(name)[0])

        cur.close()
        conn.commit()
        conn.close()        
        return info_list
        
if __name__ == '__main__':
    print GetQuery().get_data()