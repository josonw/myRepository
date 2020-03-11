#-*- coding: UTF-8 -*-
import os
import time
import sys
import re
import config

from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
path.append('%s/hadoop_libs/hive/' % os.getenv('DC_SERVER_PATH'))

import pyhs as pyhs2

from libs.DB_Mysql import Connection as m_db

class BaseHiveSql():
    """ hive sql执行类 """
    def __init__(self, day, calling = 'NoCallingGet',database='stage',task_name="python_task",eid=-1):
        """ 初始化类的参数 """
        """Hive连接类默认的计算引擎"""
        self.engine = "MR"

        self.task_name = task_name
        self.day = day
        self.calling = '%s.py' % calling    #调度模式，为1时，只做语法检查，不执行hql
        self.conn = None
        self.res = ''
        self.debug = 0
        self.hive = {'host'         : config.HIVE_DB_IP,
                    'port'          : config.HIVE_DB_PORT,
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
                    print log

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
                    print log

                if cursor.succfly_complete() is True:
                    break
            for res_row in cursor:
                yield res_row
            # except Exception, e:
            #     print "Query datas error:",e
            #     self._write_log(str(e))
            #     self._error_log(sql, str(e))
            #     self.msg = str(e)
            #     sys.exit(1)


    def _write_log(self, info):
        """ 信息打印函数，仅内部使用 """
        print '[' + time.strftime('%F %X',time.localtime()) + '] ' + info


    def _error_log(self, sql, info):
        """ 错误信息打印函数，仅内部使用 """
        try:
            mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )
            log = """%s<br>%s""" % (sql,info)
            log = log.replace("'",'"')
            log = log.replace("\n",'<br>')
            in_log = """INSERT INTO `job_log` (`eid`,`err_log`,`ftime`) VALUES (%s, '%s', unix_timestamp(now()))""" % (self.eid,log)
            mdb.execute(in_log)
            mdb.close()
        except Exception, e:
            pass

        _path = sys.path[0]
        base_path = os.getenv('SCS_PATH')
        file_name = '%s/logs/error_%s.log' % (base_path, self.day)

        try:
            f = open( file_name, 'a')
            print >> f, '%s : %s' % (self.calling, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) )
            print >> f, sql
            print >> f, info
            print >> f, ' '
            f.close()
        except Exception, e:
            pass


    def _deal_hive_log(self,log):
        mdb = None

        try:
            mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )

            p = re.finditer(r'Starting Job = job_\d+_\d+,',log)
            for match in p:
                hadoop_job_id = match.group().replace("Starting Job = ","").replace(",","")
                if hadoop_job_id and not self.start_time:
                    query_start_time = """SELECT start_time FROM `job_entity` WHERE EID = %s"""%(self.eid)
                    self.start_time = mdb.getOne(query_start_time)

                if hadoop_job_id and self.start_time:
                    relation = """INSERT INTO `entity_app`(eid,start_time,application_id,engine_type) VALUES (%s,%s,'%s','MR') ON DUPLICATE KEY UPDATE application_id= '%s' """ % (self.eid,\
                        self.start_time['start_time'],hadoop_job_id.replace("job","application"),hadoop_job_id.replace("job","application"))
                    mdb.execute(relation)
        except Exception, e:
            print "Warnning:Can not save mapreduce application ids.If this is a personal test case,you can ignore this problem:",e
        finally:
            if mdb:
                mdb.close


    def get_res(self):
        return self.res


    def get_msg(self):
        return self.msg
