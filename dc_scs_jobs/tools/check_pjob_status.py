#-*- coding: UTF-8 -*-
import os
import datetime
import time
import sys
from optparse import OptionParser

from sys import path
path.append( os.getenv('DC_SERVER_PATH') )
path.append( os.getenv('SCS_PATH') )


import config
from libs.DB_Mysql import Connection as m_db
from PublicFunc import PublicFunc
import check_sync



class ChPjobSt():

    def __init__(self, m_db):
        self.mdb = m_db(host      = config.DB_HOST,
                        database  = config.DB_NAME,
                        user      = config.DB_USER,
                        password  = config.DB_PSWD)
        # 用于判断父任务是否按时间顺序执行
        self.queue = 1

    def get_job_bind_info(self):
        """ 获取绑定信息
            初始化两组数据，self.jobbind = { cid_pid: row,}  self.cid_info = { cid:[pids, ], }
        """
        self.jobbind = {}
        self.cid_info = {}

        sql = """SELECT cid, pid, start, end, cycle, mode FROM `job_bind` """

        data = self.mdb.query(sql)
        for row in data:
            self.jobbind[ '%s_%s'%(row['cid'], row['pid']) ] = row
            if self.cid_info.get( row['cid'] ):
                self.cid_info[ row['cid'] ].append( row['pid'] )
            else:
                self.cid_info[ row['cid'] ] = [ row['pid'] ]


    def get_suspend_job(self):
        """ 获取非完成的任务包括挂起，运行
            找出最近半年运行完成的历史记录用作判断父任务是否完成
            放到内存中加快运行速度
        """
        self.readyjobs = {}
        self.jobs_jidDIC = {}    #jid, d_time为key  row为value的字典，方便快速查找
        self.jobs_running = {}
        self.jconfig = {}
        # 用于排除正在运行的任务
        data = self.mdb.query('SELECT eid, jid, calling FROM job_running')
        for row in data:
            self.jobs_running[ row['eid'] ] = row

        # 找出已实例化的任务，逐个检查是否要插入到运行表
        sql = """SELECT b.eid eid, b.jid jid, a.pri pri, a.calling calling, a.maxtime maxtime,a.jobcycle jobcycle,
                 a.maxrerun maxrerun, a.queue queue, a.u_master u_master, b.d_time d_time, b.entity_time entity_time,
                 b.entity_type start_type
                FROM `job_entity` AS b
                LEFT JOIN `jconfig` AS a ON a.jid=b.jid
                WHERE a.open=1 and b.status <> 6 """
        data = self.mdb.query(sql)
        for row in data:
            self.jobs_jidDIC[ '%s_%s'%(row['jid'], row['d_time']) ] = row

        #找出已完成的历史任务，用于排除父任务是否完成
        sql = """SELECT DISTINCT jid, d_time FROM `job_entity` WHERE status = 6"""
        data = self.mdb.query(sql)
        self.historydata = set([ '%s_%s'%(row['jid'], row['d_time']) for row in data ])

        # 各类任务配置信息
        data = self.mdb.query('SELECT jid, calling, jobcycle FROM jconfig')
        for row in data:
            self.jconfig[ row['jid'] ] = row


    def check_pjob_st(self):
        """ 检查父任务 """
        for jidday, row in self.jobs_jidDIC.items():
            if row['queue'] == self.queue:
                # 按时间执行的任务
                if not self.cid_info.get( row['jid'] ):
                    self.cid_info[ row['jid'] ] = []

                self.cid_info[ row['jid'] ].append( row['jid'] )
                self.check_interval_bybind(row['jid'], row['d_time'], row)

            else:
                if self.cid_info.get(row['jid']):
                    self.check_interval_bybind(row['jid'], row['d_time'], row)

                elif not self.jobs_running.get(row['eid']):
                    self.readyjobs[ '%s_%s'%(row['jid'], row['d_time']) ] = row


    def check_interval_bybind(self, cid, cid_dtime, row):
        """ 根据绑定信息检查父任务指定区间的完成状态flag=1,说明有父任务未完成
            有一个父任务还在 jobs_jidDIC 中的就不加到 readyjob中去
        """
        flag = 0
        for pid in self.cid_info[ cid ]:

            if pid == cid:
                for k, v in self.jobs_jidDIC.items():
                    if cid_dtime > v['d_time'] and cid == v['jid']:
                        flag =1
                        break
            else:
                key = '%s_%s' %(cid, pid)

                flag = self.do_check(cid, pid, cid_dtime, self.jobbind[ key ])

            if flag:
                break
        # 父任务度已完成，且不在运行表中
        if not flag and not self.jobs_running.get(row['eid']):
            self.readyjobs[ '%s_%s' %(cid, cid_dtime) ] = row

    def do_check(self, cid, pid, cid_dtime, jobbind_dict):
        """ 开始检查父任务
            (start, end) 属于闭型区间
            (start, end, cycle, mode)
        """
        daylist = []
        flag = 0
        start = jobbind_dict['start']
        end   = jobbind_dict['end']
        cycle = jobbind_dict['cycle']
        mode  = jobbind_dict['mode']
        # print cid, pid, cid_dtime, start, end, cycle, mode
        if mode == 'DOT' and cycle == 'D':
            # 父任务是按天间隔型的任务
            temp = set( ( '%s,%s' %(start, end) ).split(',')  )

            for i in temp:
                daylist.append( PublicFunc.add_days( cid_dtime,int(i) ) )
            pidneed_donetimes = len(daylist)

        elif mode == 'LINE' and cycle == 'D':
            # 父任务是按天连续型的任务
            st = PublicFunc.add_days( cid_dtime, int(start) )
            ed = PublicFunc.add_days( cid_dtime, int(end) )
            daylist = check_sync.AutoCompleDate(st, ed)
            pidneed_donetimes = len(daylist)

        elif mode == 'LINE' and cycle == 'W':
            # 父任务是按周连续型的任务,(start, end)
            # (start, end) = (-3,0) 表示本周之前的第三周第一天至本周最后一天
            st = PublicFunc.add_days(PublicFunc.trunc(cid_dtime, "IW"), int(start)*7 )
            ed = PublicFunc.add_days(PublicFunc.add_days(PublicFunc.trunc(cid_dtime, "IW"), int(end)*7 ), 6 )
            daylist = check_sync.AutoCompleDate(st, ed)
            pidneed_donetimes = abs(int(start) - int(end)) + 1

        elif mode == 'LINE' and cycle == 'M':
            # 父任务是按月连续型的任务
            # (start, end) = (-3,0) 表示本月之前的第三月第一天至本月最后一天
            st = PublicFunc.add_months(PublicFunc.trunc(cid_dtime, "MM"), int(start) )
            ed = PublicFunc.last_day(PublicFunc.add_months(PublicFunc.trunc(cid_dtime, "MM"), int(end) ))
            daylist = check_sync.AutoCompleDate(st, ed)
            pidneed_donetimes = abs(int(start) - int(end)) + 1

        elif mode == 'DASHED' and cycle == 'W':
            # 父任务是按周分段型的任务
            # (start, end) = (-3,0) 表示本周之前的第三周和本周
            temp = set( ('%s,%s' %(start, end)).split(',') )
            for i in temp:
                st = PublicFunc.add_days(PublicFunc.trunc(cid_dtime, "IW"), int(i)*7 )
                ed = PublicFunc.add_days(st, 6)
                daylist.extend( check_sync.AutoCompleDate(st, ed) )
            pidneed_donetimes = len(temp)

        elif mode == 'DASHED' and cycle == 'M':
            # 父任务是按月分段型的任务
            # (start, end) = (-3,0) 表示本月之前的第三月和本月
            # 如果父任务是按月执行，而且配置的是1号，所以的他跑的数据是上个月的最后一天，这里检查就不要检查父任务这个月有没有跑数据了，应该检查上个月是否有数据就可以了

            pjob_cycle = self.jconfig[pid]['jobcycle'].split(' ')
            cjob_cycle = self.jconfig[cid]['jobcycle'].split(' ')
            if len(pjob_cycle) and int(pjob_cycle[2])==1 and len(cjob_cycle) and int(cjob_cycle[2])!=1:   #父任务配置为每月一号运行的，而且子任务不是1号运行的，数据就上个月最后一天，检查上个月即可
                start = -1
                end = -1
                print 'pid:{} 配置是1号执行的,而且子任务不是1号运行的，只需要检查他上一个月是否有数据即可--{}, cid={}, d_time={}'.format(pid, pjob_cycle, cid, cid_dtime)
            temp = set( ('%s,%s' %(start, end)).split(',') )
            for i in temp:
                st = PublicFunc.add_months(PublicFunc.trunc(cid_dtime, "MM"), int(i))
                ed = PublicFunc.last_day(st)
                daylist.extend( check_sync.AutoCompleDate(st, ed) )
            pidneed_donetimes = len(temp)


        # 优先判断父任务是否完成指定时间段内的指定次数，完成则可以运行子任务，
        # 其次判断父任务是否挂起
        pidset = set( ['%s_%s'%(pid, pday) for pday in daylist] )
        pidintersect = pidset & self.historydata

        if len(pidintersect) < pidneed_donetimes:
            # flag=1 的正常情况，1，父任务没有在指定时间完成（包括未实例化，未运行完）
            # flag=1 的异常情况， 也有可能是新任务和或者周任务改成的日任务（周期粒度变为更细的任务）
            flag = 1

        if flag == 0:
            return flag

        for pday in daylist:
            if self.jobs_jidDIC.get( '%s_%s'%(pid, pday) ):
                flag = 1
                break
            elif pday == daylist[-1]:
                # 如果父任务也没有挂起，且要父任务的实例化时间小于子任务的实例化时间(可排除flag=1 的异常情况)，
                # 如果上述条件不满足只能等下轮循环检查（直至父任务完成）
                temp1 = self.jconfig[pid]['jobcycle'].split(' ')
                temp2 = self.jconfig[cid]['jobcycle'].split(' ')

                pcycle = int(temp1[1])*60 + int(temp1[0]) if temp1[0].isdigit() else int(temp1[1])*60
                ccycle = int(temp2[1])*60 + int(temp2[0]) if temp2[0].isdigit() else int(temp2[1])*60
                if pcycle < ccycle:
                    flag = 0

        return flag


    def insert_to_jobrunning(self):
        """ 将readyjob的任务插到job_running中去 """
        for jiddtime, row in self.readyjobs.items():
            jid = int(jiddtime.split('_')[0])
            pid = self.cid_info.get(jid)
            if pid and jid in pid:
                pid = list(set(pid))
                pid.remove(jid)
            if not pid:
                pid = []

            row['pjid'] = ','.join( [str(item) for item in pid] )

            # 备用  方便以后做小时级的任务
            # tmp = row['jobcycle'].split(' ')
            # if tmp[1] == tmp[2] and tmp[2] == tmp[3] and tmp[3] == tmp[4]:
            #     row['calling'] = row['calling'] + ' ' + time.strftime("%H", time.localtime())
        if self.readyjobs:
            print '\nthese jobs will run | %s' %time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
            print self.readyjobs
            # 同步实体任务，插入运行表
            data = [v for k,v in self.readyjobs.items()]
            sql = """INSERT INTO `job_running` (`eid`,`jid`,`pjid`,`queue`,`running_time`,`pri`,`calling`,`d_time`,`maxtime`,`rerun`,`status`,`u_master`)
                     VALUES (%(eid)s, %(jid)s, %(pjid)s, %(queue)s, 0, %(pri)s, %(calling)s, %(d_time)s, %(maxtime)s, 0, 2, %(u_master)s)
                  """
            self.mdb.executemany_rowcount(sql, data)

            # 同步实体任务，插入运行明细数据表
            sql = """INSERT INTO `job_detail` (`eid`,`jid`,`entity_time`, `start_time`, `end_time`, `d_time`, `start_type`)
                     VALUES (%(eid)s, %(jid)s, %(entity_time)s, 0, 0, %(d_time)s, %(start_type)s)
                  """
            self.mdb.executemany_rowcount(sql, data)


if __name__ == '__main__':

    cps = ChPjobSt(m_db)
    loop = 1
    while loop:
        time.sleep(1)
        cps.get_suspend_job()
        cps.get_job_bind_info()
        cps.check_pjob_st()
        cps.insert_to_jobrunning()

