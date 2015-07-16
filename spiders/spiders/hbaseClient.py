# -*- coding: utf-8 -*-


import sys
import json
#sys.path.append('/usr/local/lib/python2.7/site-packages')

from hbase.ttypes import IOError, AlreadyExists
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase.ttypes import ColumnDescriptor,Mutation,BatchMutation

reload(sys)
sys.setdefaultencoding('utf-8')

from hbase import Hbase

import struct

# Method for encoding ints with Thrift's string encoding
def encode(n):
    return struct.pack("i", n)

# Method for decoding ints with Thrift's string encoding
def decode(s):
    return int(s) if s.isdigit() else struct.unpack('i', s)[0]

class HBaseTest(object):
    
    def __init__(self, table='test', columnFamilies=['query:','url:','data:'],host='0.0.0.0', port=9090):
        self.table = table
        self.host = host
        self.port = port

        # Connect to HBase Thrift server
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        
        # Create and open the client connection
        self.client = Hbase.Client(self.protocol)
        self.transport.open()
        
        # set type and field of column families
        #self.set_column_families([str, str], ['name', 'sex'])
        self.set_column_families(columnFamilies)
        self._build_column_families()

    def set_column_families(self,col_list):
        self.columnFamilies = col_list

    def _build_column_families(self):
        """ give all column families name list, create a table
        """
        tables = self.client.getTableNames()
        if self.table not in tables:
            self.__create_table(self.table)
    
    def __create_table(self, table):
        """ create table in hbase with column families
        """
        columnFamilies = []
        for columnFamily in self.columnFamilies:
            name = ColumnDescriptor(name=columnFamily,maxVersions = 1)
            columnFamilies.append(name)
        try:
            self.client.createTable(table, columnFamilies)
        except AlreadyExists,tx:
            print 'Thrift exception'
            print '%s' %(tx.message)

    def close_trans(self):
        self.transport.close()

    def _del_table(self, table):
        """ Delete a table, first need to disable it.
        """
        self.client.disableTable(table)
        self.client.deleteTable(table)

    def getColumnDescriptors(self):
        return self.client.getColumnDescriptors(self.table)

    def put_Item(self,item):
        #将scrapy产生的Item存入hbase
        mutations=[]
        columnFamily=self.columnFamilies[1]
        for label in item.keys():
            if label != 'url':
                m_name=Mutation(column=columnFamily+label,value=item.get(label))
                mutations.append(m_name)

        rowKey=item.get('url','not set')
        if rowKey == 'not set':
            return
        self.client.mutateRow(self.table, rowKey, mutations, {})
              
    def getRow(self, row):
        """ get one row from hbase table

             :param row: row key
        """
        rows = self.client.getRow(self.table, row, {})
        ret = []
        for r in rows:
            rd = { 'row': r.row }
            for key in r.columns.keys():
                rd.update({key.split(':')[1]: r.columns.get(key).value })
            ret.append(rd)
        return ret

    def getRowByColumns(self,rowkey,Columns):
        rows=self.client.getRowWithColumns(self.table,rowkey,Columns,{})
        ret=[]
        for r in rows:
            rd={'row':r.row}
            for key in r.columns.keys():
                rd.update({key.split(':')[1]:r.columns.get(key).value})
            ret.append(rd)
        return ret

    def scannerWithColumns(self, needs=sys.maxint, numRows=2, startRow="", stopRow="",columns=['newsProperty:title']):
        """ scan the table
		
          :param numRows: how much rows return in one iteration.
          :param startRow: start scan row key
          :param stopRow: stop scan row key
        """
        #scan = Hbase.TScan(startRow, stopRow)
        scannerId = self.client.scannerOpenWithStop(self.table, startRow, stopRow, columns, {})
        ret = []
        rowList = self.client.scannerGetList(scannerId, numRows)
        counter=0
        while rowList and counter<needs:
            for r in rowList:
                print r
                rd = { 'row': r.row }
                for k, v in r.columns.iteritems():
                    cf, qualifier = k.split(':')
                    if qualifier not in rd:
                        rd[qualifier] = {}
                    rd[qualifier].update({ cf: v.value })
                ret.append(rd)
            counter+=len(rowList)
            rowList = self.client.scannerGetList(scannerId, numRows)
        self.client.scannerClose(scannerId)
        if len(ret)>needs:
            ret=ret[:needs]
        return ret

    def getTableReginons(self):
        return self.client.getTableRegions(self.table)
    
    def getBatchMutations(self,data):
        rowKey=data['url']
        mutations = [Mutation(column=self.columnFamilies[0]+key,value=data[key]) for key in data.keys()]
        return BatchMutation(rowKey, mutations)
   
    '''
      *Apply a series of batches in a single transaction*
      :param dataBatch:json数组
    '''
    def putsResults(self,dataBatch):
        results=[]
        for data in dataBatch:
            results.append(self.getBatchMutations(data))
        self.client.mutateRows(self.table, results, {})

def demo():
    columnFamilies=['indexData:','result']
    ht = HBaseTest(table='test', columnFamilies=columnFamilies)
#    item={}
#    item['url']='http://article.pchome.net/content-1773855.html'
#    
#    for row in ht.getRowByColumns(item['url'],columns):
#        print json.dumps(row,ensure_ascii=False)
#    print ht.getTableReginons()
#    print ht.getRow(item['url'])
    items=[]
    items.append({'url':'http://article.pchome.net/content-1773855.html','name':'name'.encode('utf8'),'key':'url'.encode('utf8')})
    items.append({'url':'http://article.pchome.net/content-1773854.html','name':'1773854'.encode('utf8')})
    ht.putsResults(items)
#    for row in ht.scannerWithColumns(needs=4,columns=['newsProperty:title']):
#        print json.dumps(row,ensure_ascii=False)
    ht.close_trans()

if __name__ == '__main__':
    demo()