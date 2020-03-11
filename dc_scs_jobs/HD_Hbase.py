#-*- coding: UTF-8 -*-

import sys
#Hbase.thrift生成的py文件放在这里
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
#如ColumnDescriptor 等在hbase.ttypes中定义
from hbase.ttypes import *
# Make socket

class Connection(object):
    """
    #ip hbase master主机ip
    #port hbase 客户端端口
    """
    def __init__(self, ip, port):
        self.ip = ip
        self.port = int(port)
        connection = TSocket.TSocket(self.ip, self.port)
        # Buffering is critical. Raw sockets are very slow
        # 还可以用TFramedTransport,也是高效传输方式
        self.transport = TTransport.TBufferedTransport(connection)
        # Wrap in a protocol
        #传输协议和传输过程是分离的，可以支持多协议
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        #客户端代表一个用户
        self.client = Hbase.Client(self.protocol)
        #打开连接
        try:
            self.transport.open()
        except Exception, e:
            raise e

    def close(self):
        self.transport.close()

    def __del__(self):
        self.close()

    def create(self, table, *familys):
        """
        创建一个hbase表
        familys 是可变的列族名称，最好不要多于3个
        """
        cols = []
        for fam in familys:
            cols.append(ColumnDescriptor( name = fam,maxVersions = 1 ))

        self.client.createTable(table, cols)

    def list(self):
        """ 返回所有表名list """
        return self.client.getTableNames()

    def put(self, table, row, col, val):
        """
        参数均为string类型
        #col 格式 '列族:列'
        """
        try:
            self.client.mutateRow(table, row, [Mutation(column=col, value=val)], {})
        except AlreadyExists, tx:
            print "Thrift exception"
            print '%s' % (tx.message)

    def delete(self, table, row, col):
        """
        删除删除某一行某一单元格
        #col 格式为 '列族:列'
        """
        return self.client.deleteAll(table, row, col, {})

    def deleteall(self, table, row):
        """
        删除删除某一行所有的单元格
        #col 格式为 '列族:列'
        """
        return self.client.deleteAllRow(table, row, {})

    def get(self, table, row):
        """
        获取某一行
        仅支持参数为string类型
        """
        return self.client.getRow(table, row, {})

    def getTCell(self, table, row, col):
        """
        获取一个单元格
        #col 格式为 '列族:列'
        """
        return self.client.get(table, row, col, {})

    def getRows(self, table, rows):
        """
        获取某几行
        #rows 格式为list
        """
        return self.client.getRows(table, rows, {})

    def scan(self, table, limit):
        """
        扫描表
        #limit 为int
        """
        scan = TScan()
        try:
            id = self.client.scannerOpenWithScan(table, scan, {})
            data = self.client.scannerGetList(id, int(limit) )
            self.client.scannerClose(id)
            return data
        except AlreadyExists, tx:
            print "Thrift exception"
            print '%s' % (tx.message)

    def is_enabled(self, table):
        return self.client.isTableEnabled(table)

    def disable(self, table):
        """ 禁用表 """
        self.client.disableTable(table)

    def enable(self, table):
        """ 启用用表 """
        self.client.enableTable(table)

    def drop(self, table):
        """ 删除表 """
        self.client.deleteTable(table)

    def incr(self, table, row, col, value):
        """
        自增
        value 为int，可以为负
        """
        return self.client.atomicIncrement(table, row, col, value)

    def get_counter(self, table, row, col):
        """
        获取自增类的值，如果其他方法获取会出十六进制字符串
        """
        return self.client.atomicIncrement(table, row, col, 0)