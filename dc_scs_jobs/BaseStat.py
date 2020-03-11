#-*- coding: UTF-8 -*-

"""
数据中心，hive调用模板
所有的sql里的表名前面必须带上库名
@
"""
# PG 参数
import os
from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
path.append('%s/tez/' % os.getenv('SCS_PATH'))

from libs import DB_PostgreSQL as pgdb
from libs import DB_Mysql as mysqldb

# Hive参数
from HiveMRSql import HiveMRSql
from BaseSparkSql import BaseSparkSql
from HiveOnTezSql import HiveOnTezSql
from PublicFunc import PublicFunc
import config

import sys
import time
import datetime

# 统一全局使用，不要每个脚本去连接了
pg_db = pgdb.Connection(host      = config.PG_DB_HOST,
                        database  = config.PG_DB_NAME,
                        user      = config.PG_DB_USER,
                        password  = config.PG_DB_PSWD)

pgcluster = pgdb.Connection(host  = config.PGCLUSTER_HOST,
                        database  = config.PG_DB_NAME,
                        user      = config.PG_SCS_USER,
                        password  = config.PG_SCS_PSWD)

mysql_db = mysqldb.Connection(host      = config.DB_HOST,
                              database  = config.DB_NAME,
                              user      = config.DB_USER,
                              password  = config.DB_PSWD)


# 统一获取时间变量函数
def _get_date_sql_dict(stat_date):
    ld_begin                = stat_date
    ld_begin_pre            = PublicFunc.add_days(stat_date, -1)
    ld_end                  = PublicFunc.add_days(stat_date, 1)
    ld_month_begin          = PublicFunc.trunc(stat_date, "MM")
    ld_1month_ago_begin     = PublicFunc.add_months(ld_month_begin, -1)
    ld_2month_ago_begin     = PublicFunc.add_months(ld_month_begin, -2)
    ld_1month_after_begin   = PublicFunc.add_months(ld_month_begin, 1)
    ld_week_begin           = PublicFunc.trunc(stat_date, "IW")
    ld_7dayago              = PublicFunc.add_days(stat_date, -7)
    ld_3dayago              = PublicFunc.add_days(stat_date, -3)
    ld_30dayago             = PublicFunc.add_days(stat_date, -30)
    ld_90dayago             = PublicFunc.add_days(stat_date, -90)

    sql_dict = {'stat_date'             :stat_date,
                'ld_begin'              :ld_begin,
                'num_begin'             :stat_date.replace('-', ''),
                'ld_begin_pre'          :ld_begin_pre,
                'ld_end'                :ld_end,
                'ld_month_begin'        :ld_month_begin,
                'ld_1month_ago_begin'   :ld_1month_ago_begin,
                'ld_2month_ago_begin'   :ld_2month_ago_begin,
                'ld_1month_after_begin' :ld_1month_after_begin,
                'ld_week_begin'         :ld_week_begin,
                'ld_7dayago'            :ld_7dayago,
                'ld_3dayago'            :ld_3dayago,
                'ld_30dayago'           :ld_30dayago,
                'ld_90dayago'           :ld_90dayago,
                }
    return sql_dict


def get_stat_date():
    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]
    return stat_date


class BaseStat():
    """所有存储过程执行的父类

    编写的存储过程类继承该类。改写 create_tab, stat 方法
    """
    def __init__(self, stat_date=datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d"), eid=0):
        """初始参数
        stat_date   传入的运算日期
        """
        self.stat_date = stat_date
        self.eid = eid

        self.get_paras()        # 参数获取
        self.hql_list = []
        self.hql_dict = _get_date_sql_dict(self.stat_date)
        # print ''
        # print 'eid = %s' % self.eid
        # print ''

    def get_paras(self):
        if len(sys.argv) == 1:
            #没有输入参数的话，日期默认取昨天
            self.stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
        elif len(sys.argv) >= 3:
            #从输入参数取，实际是第几个就取第几个
            args = sys.argv[1].split(',')
            self.stat_date = args[0]
            self.eid = int(sys.argv[2])
        else :
            args = sys.argv[1].split(',')
            self.stat_date = args[0]


    def append(self, hql):
        self.hql_list.append(hql)


    def getCalling(self):
        calling =  '%s' % self.__class__
        return calling[9:]


    def hive_init(self):
        """实例化的HiveSql类
        """
        calling = self.getCalling()

        computer_engine = "MR"

        try:
            engine_rst = mysql_db.getOne("""select engine from task_computer_engine tce join job_entity je
                on tce.jid = je.jid where je.eid=%d"""%(self.eid));
            if engine_rst:
                if engine_rst['engine']:
                    computer_engine = engine_rst['engine']
        except Exception, e:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                "Failed to get computer engine of ",self.eid," task.",e
        finally:
            mysql_db.close()

        if computer_engine == "TEZ":
            self.hq = HiveOnTezSql(self.stat_date, calling,task_name=self.__class__.__name__,eid=self.eid)
        elif computer_engine == "SPARK":
             self.hq = BaseSparkSql(self.stat_date, calling,task_name=self.__class__.__name__,eid=self.eid)
        else:
            self.hq = HiveMRSql(self.stat_date, calling,task_name=self.__class__.__name__,eid=self.eid)


    def check_input(self):
        """参数检查部分
        @后续扩展。
        """
        try:
            time.strptime(self.stat_date, '%Y-%m-%d')
        except Exception, e:
            self.write_log("the format of stat date(" + self.stat_date + ") is error, must yyyy-mm-dd")
            return 2

        return 0


    def write_log(self, info = ''):
        """打印信息
        @后续扩展
        """
        print '[' + time.strftime('%F %X',time.localtime()) + '] ' + info


    def create_tab(self):
        """创建数据表，语句
        """
        pass


    def stat(self):
        """重要部分，统计内容，编写统计内容
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # hql结尾不要带分号';'
        # 如果要更新的分区已存在数据，该语法会覆盖select查询到的分区的结果，select没有查到的分区的信息不会更新
        #一级分区动态，二级分区动态
        #用更新分区式语法插入统计数据，如果用动态分区的方式插入数据，请执行如下语句
        """
        pass

    def exe_hql(self, hql):
        res = self.hq.exe_sql(hql)
        return res

    def query(self, hql):
        datas = self.hq.query(hql)
        return datas

    def exe_hql_list(self, hql_list):
        temp_hql_list = []

        if hql_list:
            temp_hql_list = hql_list
        else:
            temp_hql_list = self.hql_list

        result = ''
        for hql_tmp in temp_hql_list:
            result = self.exe_hql(hql_tmp)
            if result != 0:
                break
        return result


    def __call__(self):
        """统计执行流程
        """
        self.hive_init()
        self.check_input()
        self.create_tab()
        self.stat()



class BasePGStat():
    """所有PG计算的基础类"""

    def __init__(self, stat_date=datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d"), eid = 0):
        self.pg = pg_db
        self.stat_date = stat_date
        self.eid = eid
        self.get_paras()        # 参数获取

        self.hql_list = []
        self.hql_dict = _get_date_sql_dict(self.stat_date)
        self.sql_dict  = self.hql_dict
        self.getCalling()

    def get_paras(self):
        if len(sys.argv) == 1:
            #没有输入参数的话，日期默认取昨天
            self.stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
        elif len(sys.argv) >= 3:
            #从输入参数取，实际是第几个就取第几个
            args = sys.argv[1].split(',')
            self.stat_date = args[0]
            self.eid = int(sys.argv[2])
        else :
            args = sys.argv[1].split(',')
            self.stat_date = args[0]

    def append(self, hql):
        self.hql_list.append(hql)


    def exe_hql(self, hql):
        # try:
        print hql
        self.pg.execute(hql)
        # except Exception, e:
        #     self._error_log(hql, str(e))
        #     print e
        #     sys.exit(1)


    def _error_log(self, sql, info):
        """ 错误信息打印函数，仅内部使用 """
        ## 调度系统日志记录到mysql
        try:
            log = """%s<br>%s""" % (sql,info)
            log = log.replace("'",'"')
            log = log.replace("\n",'<br>')
            in_log = """INSERT INTO `job_log` (`eid`,`err_log`,`ftime`) VALUES (%s, '%s', unix_timestamp(now()))""" % (self.eid,log)
            mysql_db.execute(in_log)
            mysql_db.close()
        except Exception, e:
            pass

        _path = sys.path[0]
        base_path = os.getenv('SCS_PATH')
        file_name = '%s/logs/error_%s.log' % (base_path, self.stat_date)

        try:
            f = open( file_name, 'a')
            print >> f, '%s : %s' % (self.calling, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) )
            print >> f, sql
            print >> f, info
            print >> f, ' '
            f.close()
        except Exception, e:
            pass


    def getCalling(self):
        calling =  '%s' % self.__class__        # __main__.agg_pg_paycenter_config
        self.calling = '%s.py' % calling[9:]

    """
    #执行多条语句
    sql = "insert into user (uid, name) values (%(uid)s, %(name)s)"
    data_2 = [{'uid':5, 'name':'boyaa_1'}, {'uid':6, 'name':'boyaa_2}]
    executemany(sql, data_2)
    """
    def exemany_hql(self, hql, parameters):
        try:
            self.pg.executemany(hql, parameters)
        except Exception, e:
            print e
            sys.exit(1)


    def query(self, hql):
        datas = self.pg.query(hql)
        return datas

    def exe_hql_list(self, hql_list=[]):
        temp_hql_list = []

        if hql_list:
            temp_hql_list = hql_list
        else:
            temp_hql_list = self.hql_list

        for hql_tmp in temp_hql_list:
            #函数有退出机制,不需要做异常判断
            self.exe_hql(hql_tmp)



    def stat(self):
        raise NotImplementedError("Subclasses should implement this!")

    def __call__(self):
        """统计执行流程
        """
        self.stat()



class BasePGCluster():
    """所有PG集群计算的基础类"""

    def __init__(self, stat_date=datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d"), eid = 0):
        self.pg = pgcluster
        self.stat_date = stat_date
        self.eid = eid
        self.get_paras()        # 参数获取

        self.hql_list = []
        self.hql_dict = _get_date_sql_dict(self.stat_date)
        self.sql_dict  = self.hql_dict
        self.getCalling()

    def get_paras(self):
        if len(sys.argv) == 1:
            #没有输入参数的话，日期默认取昨天
            self.stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
        elif len(sys.argv) >= 3:
            #从输入参数取，实际是第几个就取第几个
            args = sys.argv[1].split(',')
            self.stat_date = args[0]
            self.eid = int(sys.argv[2])
        else :
            args = sys.argv[1].split(',')
            self.stat_date = args[0]

    def append(self, hql):
        self.hql_list.append(hql)


    def exe_hql(self, hql):
        # try:
        session_param = """
        set gp_autostats_mode='on_change';
        set gp_autostats_on_change_threshold=10000;"""
        print "session_param :%s"%session_param
        self.pg.execute(session_param)

        print hql
        self.pg.execute(hql)
        # except Exception, e:
        #     self._error_log(hql, str(e))
        #     print e
        #     sys.exit(1)


    def _error_log(self, sql, info):
        """ 错误信息打印函数，仅内部使用 """
        ## 调度系统日志记录到mysql
        try:
            log = """%s<br>%s""" % (sql,info)
            log = log.replace("'",'"')
            log = log.replace("\n",'<br>')
            in_log = """INSERT INTO `job_log` (`eid`,`err_log`,`ftime`) VALUES (%s, '%s', unix_timestamp(now()))""" % (self.eid,log)
            mysql_db.execute(in_log)
            mysql_db.close()
        except Exception, e:
            pass

        _path = sys.path[0]
        base_path = os.getenv('SCS_PATH')
        file_name = '%s/logs/error_%s.log' % (base_path, self.stat_date)

        try:
            f = open( file_name, 'a')
            print >> f, '%s : %s' % (self.calling, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) )
            print >> f, sql
            print >> f, info
            print >> f, ' '
            f.close()
        except Exception, e:
            pass


    def getCalling(self):
        calling =  '%s' % self.__class__        # __main__.agg_pg_paycenter_config
        self.calling = '%s.py' % calling[9:]

    """
    #执行多条语句
    sql = "insert into user (uid, name) values (%(uid)s, %(name)s)"
    data_2 = [{'uid':5, 'name':'boyaa_1'}, {'uid':6, 'name':'boyaa_2}]
    executemany(sql, data_2)
    """
    def exemany_hql(self, hql, parameters):
        try:
            self.pg.executemany(hql, parameters)
        except Exception, e:
            print e
            sys.exit(1)


    def query(self, hql):
        datas = self.pg.query(hql)
        return datas

    def exe_hql_list(self, hql_list=[]):
        temp_hql_list = []

        if hql_list:
            temp_hql_list = hql_list
        else:
            temp_hql_list = self.hql_list

        for hql_tmp in temp_hql_list:
            #函数有退出机制,不需要做异常判断
            self.exe_hql(hql_tmp)



    def stat(self):
        raise NotImplementedError("Subclasses should implement this!")

    def __call__(self):
        """统计执行流程
        """
        self.stat()
