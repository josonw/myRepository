#-*- coding: UTF-8 -*-
import os
import time
import sys
import re
import config
import datetime
from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
path.append('%s/hadoop_libs/hive/' % os.getenv('DC_SERVER_PATH'))

import pyhs as pyhs2

from libs.DB_Mysql import Connection as m_db

class BaseSparkSql():
    """ hive sql执行类 """
    def __init__(self, day, calling = 'NoCallingGet',database='stage',task_name="python_task",eid=-1):
        """ 初始化类的参数 """
        """Hive连接类默认的计算引擎"""
        self.engine = "MR"

        self.task_name = task_name
        self.stat_date = day if day else datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
        self.calling = '%s.py' % calling    #调度模式，为1时，只做语法检查，不执行hql
        self.conn = None
        self.res = ''
        self.debug = 0
        self.hive = {'host'         : config.HIVE_DB_IP,
                    'port'          : config.SPARK_DB_PORT,
                    'authMechanism' : config.HIVE_DB_AUTH,
                    'user'          : config.HIVE_DB_USER,
                    'password'      : config.HIVE_DB_PSWD,
                    'configuration' : None
                    }
        self.eid = eid
        self.start_time = None
        self.hive.update({'database': database})

        # 初始化hive链接
        try:
            self.reconnect()
        except Exception, e:
            print 'connect Error : %s ' % e

        self.init_properties()


    """
    初始化hive的配置参数
    """
    def init_properties(self):
        pass


    def __del__(self):
        """ 做一些清理操作 """
        self.close()


    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.conn = pyhs2.connect(host=self.hive['host'], port=self.hive['port'], user=self.hive['user'],
            password=self.hive['password'], database=self.hive['database'], authMechanism=self.hive['authMechanism'],configuration=self.hive['configuration'])


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


    """
    执行hive sql操作
    """
    def exe_sql(self, hql, debug=False):
        """
        执行一条sql，比如update, insert, delete等操作。查询select请使用 query，get, iter
        """
        #执行之前去掉hql结尾的分号;
        hql = hql.strip().strip(';')
        cursor = self._cursor()
        for sql in hql.split(';'):
            if self.debug or debug:
                if 'set hive' not in sql:
                    sql = 'explain '+ sql
            self._write_log(sql)

            # try:
            cursor.execute_async(sql)
            while True:
                log = cursor.fetch_log()
                if log:
                    self._deal_hive_log(log)
                    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                        log

                if cursor.succfly_complete() is True:
                    break
            # except Exception, e:
            #     self._write_log(str(e))
            #     self._error_log(sql, str(e))
            #     self.msg = str(e)
            #     sys.exit(1)

        return 0


    """
    获取hive查询结果

    返回：
        生成器，大数据时内存消耗小
    """
    def query(self, sql):
        """
        Query hive data and return result set.
        The max rows account is 10000.

        Considering performance,
        the return value is a generator(生成器).
        """
        with self.conn.cursor() as cursor:
            self._write_log(sql)

            # try:
            query_datas = cursor.query_async(sql)
            while True:
                log = cursor.fetch_log()
                if log:
                    self._deal_hive_log(log)
                    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                        log

                if cursor.succfly_complete() is True:
                    break
            for res_row in cursor:
                yield res_row


    def _write_log(self, info):
        """ 信息打印函数，仅内部使用 """
        print '[' + time.strftime('%F %X',time.localtime()) + '] ' + info


    def _deal_hive_log(self,log):
        pass


    def get_res(self):
        return self.res


    def get_msg(self):
        return self.msg
