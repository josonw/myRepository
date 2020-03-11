#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
创建于2016-01-18
@作者:蒋志兴1885
@简介：监控同步任务
"""

import sys
import os
from sys import path
import time
import datetime

path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from libs import DB_Mysql as mysqldb
from libs import DB_PostgreSQL as pgdb
from dc_scs_jobs.BaseHiveSql import BaseHiveSql

from libs.warning_way import send_sms
import dc_scs_jobs.config as config
from dc_scs_jobs.stat_sync import Sync


# 中文解码解决方案，指定默认字符集
reload(sys)
sys.setdefaultencoding('utf-8')


def AutoCompleDate(sdate, edate):
    """返回开始日期到结束日期的的所有日期
    """
    s_date = datetime.datetime.strptime(sdate,'%Y-%m-%d')
    e_date = datetime.datetime.strptime(edate,'%Y-%m-%d')
    days = (e_date - s_date).days
    dates = [ (s_date + datetime.timedelta(days = d)).strftime('%Y-%m-%d') for d in xrange(days+1) ]
    return dates


class monitor_export_result():
    """ 监控同步任务执行结果，检查hive表和pg表的行数是否相同
        每隔一段时间从job_entity表中根据 start_time,d_time检查一次新跑过的同步任务
        目前每天会有230个左右的同步任务会执行（不计人工重跑的同步任务）
    """
    def __init__(self, pgdb, mysqldb, BaseHiveSql, day):
        """ 数据库连接初始化"""
        self.pdb = pgdb.Connection(host      = config.PG_DB_HOST,
                                   database  = config.PG_DB_NAME,
                                   user      = config.PG_DB_USER,
                                   password  = config.PG_DB_PSWD)

        self.mdb = mysqldb.Connection(host      = config.DB_HOST,
                                      database  = config.DB_NAME,
                                      user      = config.DB_USER,
                                      password  = config.DB_PSWD)
        self.hdb = BaseHiveSql(day)

        self.chk_result = []


    def get_check_jobs(self):
        """找出job_entity中运行时间在检查时间之后的同步表"""
        sql = """ SELECT jid, calling, u_master, start_time, d_time, check_dtfield, tbl_name
                  FROM
                      (SELECT a.jid, a.calling, a.u_master, b.start_time, b.d_time,c.check_dtfield,c.tbl_name
                      FROM job_entity b
                      INNER JOIN jconfig a ON b.jid = a.jid
                      INNER JOIN sync_hive_pg_check c ON SUBSTRING_INDEX(a.calling,' ', -1) = c.tbl_name
                      WHERE b.status=6 AND b.start_time > c.check_time AND c.tbl_type <> 3
                            AND (c.flag ='ALL' OR LOCATE('-',c.flag))
                      ORDER BY b.eid DESC) d
                  GROUP BY calling
                  UNION ALL
                  SELECT a.jid, a.calling, a.u_master, b.start_time, b.d_time,c.check_dtfield,c.tbl_name
                  FROM job_entity b
                  INNER JOIN jconfig a ON b.jid = a.jid
                  INNER JOIN sync_hive_pg_check c ON SUBSTRING_INDEX(a.calling,' ', -1) = c.tbl_name
                  WHERE b.status=6 AND b.start_time > c.check_time AND c.tbl_type <> 3
                        AND c.flag <>'ALL' AND LOCATE('-',c.flag)=0
              """
        # select a.jid, a.calling, a.u_master, a.jobcycle ,c.* from jconfig a
        # left join sync_hive_pg_check c ON SUBSTRING_INDEX(a.calling,' ', -1) = c.tbl_name
        # where  LOCATE('-',c.flag) and a.open=1;
        h = int(time.strftime("%H", time.localtime()))
        # 9点之前只检查数据日期是前一天有过更新的同步表，
        # 9点之后检查所有有过更新的同步表，防止有些同步任务人工重跑
        if len(sys.argv) == 2 and h < 9:
            sql = sql + " AND b.d_time = '%s' "%sys.argv[1]

        self.check_jobs = self.mdb.query(sql)
        if self.check_jobs:
            tbl_name = list(set([row['tbl_name'] for row in self.check_jobs]))
            self.mdb.execute("UPDATE sync_hive_pg_check SET check_st = 0 WHERE tbl_name in (%s) " \
                              %("'" + "','".join(tbl_name) + "'"))

    def init_table_info(self, tablename, d_time):
        """初始化表信息"""
        self.syn = Sync('export',tablename)
        self.d_time = d_time

        if self.syn.flag.upper() !='ALL':
            self.dates, self.check_type = self.syn._get_sync_days(d_time)
        else:
            self.check_type = ''

        if self.check_type == 'line':
            tmp = AutoCompleDate(self.dates[0],self.dates[1])
            self.dates = tmp[0:-1]   #左闭右开区间

    def get_sql_cmd(self, check_dtfield):
        """获取对应的sql语句"""
        if self.syn.flag.upper()  == 'ALL':
            hql_cmd = 'SELECT COUNT(*) AS cn FROM %s.%s' % (self.syn.schema,self.syn.tbl_name)
            pql_cmd = hql_cmd

        else:
            # 因为日期有可能是多个的非连续日期，所以暂时先用in条件来判断
            if check_dtfield ==11:
                hql_date = "cast('" + "' as date),cast('".join(self.dates) + "' as date)"
                pql_date = "'" + "'::date,'".join(self.dates) + "'::date"
                hql_cmd = """SELECT COUNT(*) AS cn FROM %s.%s WHERE %s in (%s) """ \
                           % (self.syn.schema, self.syn.tbl_name, 'dt', hql_date)

                pql_cmd = """SELECT COUNT(*) AS cn FROM %s.%s WHERE %s in (%s) """ \
                           % (self.syn.schema, self.syn.tbl_name, self.syn.partition_field, pql_date)

            elif check_dtfield ==12:
                hql_date = "cast('" + "' as date),cast('".join(self.dates) + "' as date)"
                pql_date = "'" + "','".join(self.dates) + "'"
                hql_cmd = """SELECT COUNT(*) AS cn FROM %s.%s WHERE %s in (%s) """ \
                           % (self.syn.schema, self.syn.tbl_name, 'dt', hql_date)

                pql_cmd = """SELECT COUNT(*) AS cn FROM %s.%s WHERE to_char(%s,'YYYY-MM-DD') in (%s) """ \
                           % (self.syn.schema, self.syn.tbl_name, self.syn.partition_field, pql_date)

            elif check_dtfield ==13:
                hql_date = "cast('" + "' as date),cast('".join(self.dates) + "' as date)"
                pql_date = "'" + "','".join(self.dates) + "'"
                hql_cmd = """SELECT COUNT(*) AS cn FROM %s.%s WHERE %s in (%s) """ \
                           % (self.syn.schema, self.syn.tbl_name, 'dt', hql_date)

                pql_cmd = """SELECT COUNT(*) AS cn FROM %s.%s WHERE %s in (%s) """ \
                           % (self.syn.schema, self.syn.tbl_name, self.syn.partition_field, pql_date)

            elif check_dtfield ==21:
                hql_date = "'" + "','".join(self.dates) + "'"
                pql_date = "'" + "'::date,'".join(self.dates) + "'::date"
                hql_cmd = """SELECT COUNT(*) AS cn FROM %s.%s WHERE %s in (%s) """ \
                           % (self.syn.schema, self.syn.tbl_name, 'dt', hql_date)

                pql_cmd = """SELECT COUNT(*) AS cn FROM %s.%s WHERE %s in (%s) """ \
                           % (self.syn.schema, self.syn.tbl_name, self.syn.partition_field, pql_date)

            elif check_dtfield ==22:
                hql_date = "'" + "','".join(self.dates) + "'"
                pql_date = hql_date
                hql_cmd = """SELECT COUNT(*) AS cn FROM %s.%s WHERE %s in (%s) """ \
                           % (self.syn.schema, self.syn.tbl_name, 'dt', hql_date)

                pql_cmd = """SELECT COUNT(*) AS cn FROM %s.%s WHERE to_char(%s,'YYYY-MM-DD') in (%s) """ \
                           % (self.syn.schema, self.syn.tbl_name, self.syn.partition_field, pql_date)

            elif check_dtfield ==23:
                hql_date = "'" + "','".join(self.dates) + "'"
                pql_date = hql_date
                hql_cmd = """SELECT COUNT(*) AS cn FROM %s.%s WHERE %s in (%s) """ \
                           % (self.syn.schema, self.syn.tbl_name, 'dt', hql_date)

                pql_cmd = """SELECT COUNT(*) AS cn FROM %s.%s WHERE %s in (%s) """ \
                           % (self.syn.schema, self.syn.tbl_name, self.syn.partition_field, pql_date)

        return  hql_cmd,pql_cmd

    def is_syncjob_ok(self, hql_cmd, pql_cmd, umaster):
        """ 执行sql语句 并检查返回的行数"""
        try:
            hcn = self.hdb.query(hql_cmd)
            pcn = self.pdb.get(pql_cmd)
            hresult = hcn.next()
            # 将4种检查结果更新到数据库
            if not hresult and not pcn:     # 都为空
                flag = 21
                print "%s.%s: %s is ok "%(self.syn.schema, self.syn.tbl_name, self.d_time)

            elif not hresult or not pcn:      # 有一个为空，另一个非空
                flag = 12
                self.chk_result.append("%s: %s, %s, %s, %s"%(self.syn.tbl_name, self.d_time, umaster, hresult[0], pcn['cn']))

            elif hresult[0] == pcn['cn']:
                flag = 22
                print "%s.%s: %s is ok "%(self.syn.schema, self.syn.tbl_name, self.d_time)

            else:
                flag = 11
                self.chk_result.append("%s: %s, %s, %s, %s"%(self.syn.tbl_name, self.d_time, umaster, hresult[0], pcn['cn']))

            # 更新检查时间
            usql = "UPDATE sync_hive_pg_check SET check_time = %s, check_st=%s WHERE tbl_name ='%s' " \
                   %(time.time(),flag, self.syn.tbl_name)

            self.mdb.execute(usql)

        except Exception, e:
            print 'checkjob have error:\n'
            print hql_cmd
            print pql_cmd
            print str(e)

if __name__ == '__main__':

    str_day = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    print 'check sync jobs begin | check start time is %s' %(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))
    mer = monitor_export_result(pgdb, mysqldb, BaseHiveSql, str_day)
    mer.get_check_jobs()

    for row in mer.check_jobs:
        tbl_name = row['tbl_name'].strip()
        dtime = row['d_time'].strip()
        umaster = row['u_master'].strip()

        mer.init_table_info(tbl_name,dtime)
        hcmd, pcmd = mer.get_sql_cmd(row['check_dtfield'])
        mer.is_syncjob_ok(hcmd, pcmd, umaster)

    if mer.chk_result:
        warning_msg = u"""同步告警,此次检查%s个表,下表同步行数不一致:\n""" %len(mer.check_jobs) + \
                      u"""[表名,统计日期,关注人,h行数,p行数]\n""" + \
                      '\n'.join(mer.chk_result)

        print warning_msg
        print '%s sync jobs have wrong | check over time is %s' %(len(mer.check_jobs),time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))
        send_sms(config.CONTACTS_LIST_ENAME, warning_msg)

    else:
        print 'all sync jobs are successful | check over time is %s' %(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))
