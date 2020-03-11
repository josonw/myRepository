#-*- coding: UTF-8 -*-
import os
import time
import sys
sys.path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from impala.dbapi import connect

class ImpalaSql(object):
    """ Impala sql执行类 """
    def __init__(self, host, port, **args):
        """ 初始化类的参数 """
        self.host = host
        self.port = port

        # 初始化hive链接
        try:
            self.reconnect()
        except Exception, e:
            print 'connect Error : %s ' % e


    def __del__(self):
        """ 做一些清理操作 """
        self.close()

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.conn = connect(host=self.host,
            port=self.port)

    def close(self):
        """ close connection """
        self.conn.close()
        self.conn = None

    def _ensure_connected(self):
        if (self.conn is None):
            self.reconnect()

    def _cursor(self):
        self._ensure_connected()
        return self.conn.cursor()

    def query(self, sql):
        """
        获取impala查询结果

        返回：
            生成器，大数据时内存消耗小
        """
        with self.conn.cursor() as cursor:
            try:
                query_datas = cursor.execute(sql)
                for res_row in cursor:
                    yield res_row
            except Exception, e:
                print "Query datas error:", e
                sys.exit(1)

    def execute(self, sql):
        """
        执行insert update 等sql
        """
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(sql)
            except Exception, e:
                print "execute sql error:", e
                sys.exit(1)


class Row(dict):
    """访问对象那样访问dict,行结果"""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
