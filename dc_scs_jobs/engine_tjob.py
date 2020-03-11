#-*- coding: UTF-8 -*-
import time
import multiprocessing
import os
import sys
import signal
import subprocess
import traceback
from itertools import chain

from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
path.append('%s/tez/' % os.getenv('SCS_PATH'))

from libs.DB_Mysql import Connection as m_db
from libs.warning_way import send_sms
import BaseStat
from ResourceSchedule import ResourceSchedule
import config



reload(sys)
sys.setdefaultencoding( "utf-8" )


class Tempworker():
    """ 临时工作多进程调度类（业务导数，临时查询） """
    def __init__(self, mdb):
        self.maxhadoop_pn = 10                             #最大hadoop任务数
        self.maxother_pn = 10                             #最大其他任务数
        self.hadoop_pn = 0                             #hadoop任务数
        self.other_pn = 0                              #其他任务数

        self.isAlarm = 1                            #是否开启告警 1开启 0关闭

        self.queue = multiprocessing.Queue()        #进程间通信用 记录完成任务的jid
        self.queerr = multiprocessing.Queue()        #存储脚本执行异常的任务
        self.mdb = mdb
        self.record = {}                            #存储进程信息
        self.jobs = {}                                #存储待处理的任务信息
        self.readyJobs = {}                            #存储已就绪的任務
        self.hadoop_init()



    def refreshJobs(self):
        """ 刷新任务信息 """
        self.jobs = {}
        self.readyJobs = {}
        sql = """SELECT jid, jobname, tempsql, queue, running_time, pri, calling, d_time,
                 maxtime, rerun, status, u_master, hadoop_sch, alarm, maxrerun, debug, jobtype, exp_time,regap
                 FROM `tjob_running`
              """
        data = self.mdb.query(sql)
        for row in data:
            if row.get('jid'):
                self.jobs[ row.get('jid') ] = row
            else:
                print 'mysql select from tjob_running error:'
                print row
    def getQueueSize(self):
        """ 获取已完成未join的任务个数 """
        return self.queue.qsize()

    def gethadooppn(self):
        """ 获取hadoop进程数 """
        return self.hadoop_pn

    def getotherpn(self):
        """ 获取非hadoop进程数 """
        return self.other_pn

    def getJobs(self):
        """ 获取任务信息 """
        return self.jobs

    def proc_num_update(self):
        """进程数根据mode,num,jid发生相应更新"""

        sql = "select count(*) as run_num from tjob_running where status=3 "
        data_dict = self.mdb.getOne(sql)
        self.hadoop_pn = data_dict.get('run_num', 0)

        proc_info = """The current hadoop tempjob nums: %s, other tempjobs nums: %s | %s""" \
                    %(self.hadoop_pn,self.other_pn,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))
        print proc_info

    def getReadyJobs(self):
        """ 获取就绪的任务信息 """
        return self.readyJobs


    def get_detail(self, jid):
        """ 获取明细表中的最近的did entity_time"""
        sql = """SELECT *
                 FROM `tjob_detail` WHERE jid = %s ORDER BY did DESC LIMIT 1""" % jid

        data = self.mdb.getOne(sql)
        if not data:
            return False
        return data

    def insert_detail(self, jids):
        """ 同步并插入明细表，一个任务的就绪准备 """
        #start_type'启动方式： 11：凌晨任务自启；12：超时后自启；13：出错后自启；4：页面启动',
        jids_list = str(jids)

        data = self.mdb.getOne(""" select * from tjob_running where jid in (%s) """ %jids_list)
        data['tempsql'] = self.mdb._db.escape_string(data['tempsql'] )
        sql = """ INSERT INTO `tjob_detail` (`jid`, `jobname`, `tempsql`,`queue`,`running_time`,`pri`,`calling`,`d_time`,
                                             `maxtime`,`rerun`,`status`,`u_master`,
                                             `alarm`,`end_time`,`maxrerun`,`debug`,`jobtype`,`exp_time`,`regap`)
                     VALUES  (%(jid)s, '%(jobname)s', '%(tempsql)s', %(queue)s, 0, %(pri)s,'%(calling)s', '%(d_time)s',
                              %(maxtime)s, %(rerun)s, %(status)s, '%(u_master)s',
                              %(alarm)s, 0, %(maxrerun)s, %(debug)s,'%(jobtype)s',%(exp_time)s,%(regap)s) """%data

        self.mdb.execute(sql)

    def update_detail(self, did, start_time):
        """ 同步更新明细表，更新一个任务的开启时间 """
        sql = """ UPDATE `tjob_detail` SET running_time = %s WHERE did = %s""" %( start_time, did)
        self.mdb.execute(sql)


    def overtjob_detail(self, did, end_time, status=None):
        """ 同步更新明细表，一个任务的结束"""
        #end_type  '结束方式：4：超时；5：出错；6：完成；7：停止'  值和tjob_running表同步
        if status:
            sql = """ UPDATE `tjob_detail` SET end_time = %s,status =%s WHERE did = %s""" %( end_time,status,did)
        else:
            sql = """ UPDATE `tjob_detail` SET end_time = %s WHERE did = %s""" %( end_time, did)
        self.mdb.execute(sql)

    def CleanMode(self):
        """开启清理模式"""
        #更新hadoop任务运行记录
        h = time.strftime("%H", time.localtime())
        tm = int(time.time())
        # 每晚22点以后，停止所有的临时任务,23点以后删除所有过期任务
        if self.jobs and int(h) >= 22:
            if int(h)<23:
                jids = self.mdb.query(""" select jid from tjob_running where status <> 7""")
                jids = ','.join([str(item['jid']) for item in jids])
                if jids:
                    # 推送的任务不要暂停,资源消耗可控
                    sql1 = """ UPDATE tjob_running set status=7 where jid in (%s) and jobtype<>'pushjobs' """%jids
                    sql2 = """ UPDATE tjob_detail set status=7 where jid in (%s) and jobtype<>'pushjobs' """%jids
                    self.mdb.execute(sql1)
                    self.mdb.execute(sql2)

            elif int(h) >= 23:
                self.mdb.execute(""" delete from tjob_running where UNIX_TIMESTAMP(now())-UNIX_TIMESTAMP(d_time) > exp_time*60*60""")

    def writeLog(self, eid, msg):
        """ 错误日志记录到mysql """
        in_log = """insert INTO `job_log` (`eid`,`err_log`,`ftime`)
                   VALUES (%%s, '%(err_log)s',unix_timestamp(now()))
                 """ % {'err_log':self.mdb._db.escape_string(msg)}
        try:
            self.mdb.execute_rowcount(in_log,(eid,) )
            flag = 'success'
        except Exception, e:
            errmsg = traceback.format_exc()
            print 'wrongmsg is: \n %s' %errmsg
            print 'insert sql is: \n %s' %in_log
            flag ='false'
        finally:
            print 'tjob err_log mysqldb insert %s, jid is %s| %s'%(flag, eid, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()) )


    def doAlarm(self, jid, msg):
        """ 封装告警发送 """
        if 0 == self.isAlarm:
            return False

        info = self.jobs.get(jid, {'jid':0})  #容错
        if 0 == info['jid']:
            return False

        if 1 == info['alarm']:      #暂时只有短信告警
            return False

        members = self.mdb.query("select user from loop_member where priority=1")
        users=[item for item in info['u_master'].split(',')]
        users.extend([item.get('user','') for item in members])

        send_sms(users, msg, 3)
        # print 'priority is %s,num is %s' %(priority,num)
        return True


    def startJob(self, jid):
        """ 开启新的子进程 """
        process = multiprocessing.Process(target=self.worker,args=(jid,))
        process.start()
        self.record[jid] = process
        run_sql = """UPDATE `tjob_running` SET status=3, running_time=%s WHERE jid= %s""" % (int(time.time()),jid)
        self.mdb.execute(run_sql)
        run_sql = """UPDATE `tjob_detail` SET status=3, running_time=%s WHERE jid= %s""" % (int(time.time()),jid)
        self.mdb.execute(run_sql)
        detaildata = self.get_detail(jid)
        if detaildata:
            self.update_detail(detaildata['did'], int(time.time()))

    def joinJob(self):
        """ 回收正常完成的子进程 """
        if 0 < self.getQueueSize():
            jid = self.queue.get()
            if self.record.get(jid, 0) != 0:    #容错
                self.record[jid].join()
                self.proc_num_update()    #进程数更新

                detaildata = self.get_detail(jid)
                if detaildata:
                    self.overtjob_detail(detaildata['did'], int(time.time()),6)

                self.mdb.execute('DELETE FROM `tjob_running` WHERE jid= %s' % jid)

                del self.record[jid]            #注销进程信息
                del self.jobs[jid]

        if 0 < self.queerr.qsize():
            jid = self.queerr.get()
            if self.record.get(jid, 0) != 0:    #容错
                self.record[jid].join()
                self.proc_num_update()    #进程数更新

                warning_msg = '查询任务出错：%s' % self.jobs.get(jid,{'jid':'jid-%s'%jid})['jid']
                self.doAlarm(jid, warning_msg)

                detaildata = self.get_detail(jid)
                if detaildata:
                    self.overtjob_detail(detaildata['did'], int(time.time()),5)

                self.mdb.execute('UPDATE `tjob_running` SET status=5, running_time=%s WHERE jid= %s' % (int(time.time()),jid) )
                del self.record[jid]            #注销进程信息
                self.jobs[jid]['status'] = 5

    def handle_timeout(self, jid):
        self.proc_num_update()    #进程数更新

        detaildata = self.get_detail(jid)
        if detaildata:
            self.overtjob_detail(detaildata['did'], int(time.time()), 4)

        maxrerun = self.jobs[jid]['maxrerun']
        rerun = self.jobs[jid]['rerun']          #看下重启次数
        if rerun >= maxrerun:    #重跑超过次数
            out_sql = """UPDATE `tjob_running` SET status=5 WHERE jid= %s """ %jid
            self.jobs[jid]['status'] = 4
            #下面发告警
            warning_msg = '查询任务重启超阀：%s' % self.jobs[jid]['jobname']
            self.doAlarm(jid, warning_msg)
            print 'tempjob rerun out maxreun jid= %s' % jid
        else:
            out_sql = """UPDATE `tjob_running` SET status=4, running_time = %s WHERE jid= %s """ % (int(time.time()), jid)
            self.jobs[jid]['running_time'] = int(time.time())
            self.jobs[jid]['status'] = 4
            #下面发告警
            warning_msg = '查询任务超时：%s' % self.jobs[jid]['jobname']
            # self.doAlarm(jid, warning_msg)
            print 'tempjob run out maxtime jid= %s' % jid
        self.mdb.execute(out_sql)


    def checkTimeOut(self):
        """ 超时检查，超时的任务中止并回收 """
        temp = []
        for jid,val in self.record.items():
            if not self.jobs.get(jid):
                # 防止出现keyerr 错误
                continue

            out_time = int(time.time()) - self.jobs[jid]['running_time']
            if out_time > self.jobs[jid]['maxtime'] + 1800:
                # 超时30分钟后将被强杀,连带子任务进程
                cmd0 = 'ps -ef | grep python272 | grep hadoop | grep -v 127.0 | grep -v grep | grep -v supervisord |grep %s' %val.pid
                cmd1 = cmd0 + " | awk '{print $2}' | xargs kill -9 "
                record_killed = os.popen(cmd0).read()
                print '\nThe following tempjob process will be forced killed | %s\n' \
                      % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())),record_killed
                result = os.popen(cmd1).read()
                print result
                temp.append(jid)    #临时表上
                self.handle_timeout(jid)

            elif out_time > self.jobs[jid]['maxtime']:
                val.terminate()    #退出超时的子进程
                val.join(0.5)         #限时一秒回收子进程，防止阻塞主进程
                if val.is_alive():   #如果没有回收掉该进程继续判断下一个
                    print 'tempjob subproc pid: %s, jid: %s run out maxtime can not stop by join' %(val.pid, jid)
                    continue
                temp.append(jid)    #临时表上
                self.handle_timeout(jid)

        #进程信息中删除已重启的
        for jid in temp:
            del self.record[jid]
            self.writeLog(jid, '任务超时！')

    def checkReady(self):
        """ 就绪检查，修改任务状态：1：挂起；2：就绪；3：运行；4：超时；5：出错；6：完成；7：停止 """
        temp = []    #已完成的先装进来，循环完后删除
        for jid,val in self.jobs.iteritems():
            # 防止出现 KeyErr 的错误
            if not (val.get('jid') and val.get('status') and val.get('d_time')):
                continue

            #停止的任务，如果进程开启了就杀掉，没开进程的跳过
            if 7 == val['status']:
                if self.record.get(jid, 0) != 0:
                    self.record[jid].terminate()    #杀掉子进程
                    self.record[jid].join(0.5)         #限时一秒停止子进程，防止阻塞主进程
                    if self.record[jid].is_alive():   #如果这次没有回收掉该进程，继续判断下一个
                        print 'tempjob subproc pid: %s, jid: %s can not stop by join' %(self.record[jid].pid, jid)
                        continue
                    self.proc_num_update()    #进程数更新
                    detaildata = self.get_detail(jid)
                    if detaildata:
                        self.overtjob_detail(detaildata['did'], int(time.time()), 7)
                    del self.record[jid]    #进程信息中删除
                continue

            #已完成的清理掉
            if 6 == val['status']:
                sql = """UPDATE `tjob_detail` SET  end_time=%s and status=6 WHERE jid = %s""" % (int(time.time()),jid)
                self.mdb.execute(sql)
                self.mdb.execute('DELETE FROM `tjob_running` WHERE jid= %s' % jid)
                temp.append(jid)

            #出错的任务重启
            if 5 == val['status']:
                regap = self.jobs[jid]['regap']
                if (int(time.time()) - val['running_time']) < regap:
                    continue
                rerun = self.jobs[jid]['rerun']         #看下重启次数
                if rerun >= self.jobs[jid]['maxrerun']:
                    continue

                #已经过了缓冲时间并且小于重启次数，改成挂起状态,重跑
                self.jobs[jid]['status'] = 1
                sql = """UPDATE `tjob_running` SET status = 2, rerun = %s WHERE jid = %s""" % (rerun + 1, jid)
                self.insert_detail(jid)
                self.mdb.execute(sql)

            #超时过的任务比较特殊，最少等上次结束x分钟后再跑
            if 4 == val['status']:
                regap = self.jobs[jid]['maxtime']
                if (int(time.time()) - val['running_time']) < regap:
                    continue
                else:    #已经过了缓冲时间，改成挂起状态
                    self.jobs[jid]['rerun'] += 1
                    self.jobs[jid]['status'] = 1
                    sql = """UPDATE `tjob_running` SET status = 2, rerun = rerun + 1  WHERE jid = %s""" % jid
                    self.insert_detail(jid)
                    self.mdb.execute(sql)

            #运行中的检查下是否与进程信息匹配, 不匹配的改成出错
            if 3 == val['status']:
                if '' == self.record.get(jid, ''):
                    self.mdb.execute("""UPDATE `tjob_running` SET status = 5 WHERE jid = %s""" % jid)
                    print 'loop进程 任务调度出错！'
                else:  #更新hadoop任务进度
                    hadoopsch = self.hadooprs.get_task_progress(jid)
                    self.mdb.execute("""UPDATE `tjob_running` SET hadoop_sch = %s WHERE jid = %s""" %(hadoopsch['vcores'], jid))

            #本来就是已就绪的
            if 2 == val['status']:
                self.readyJobs[jid] = val

            if 1 == val['status']:    #判断父任务都有木有完成
                self.jobs[jid]['status'] = 2
                sql = """UPDATE `tjob_running` SET status = 2, rerun = rerun + 1  WHERE jid = %s""" % jid
                self.mdb.execute(sql)
        #删除已完成的任务
        for jid in temp:
            del self.jobs[jid]


    def checkJobs(self):
        """ 任务检查，超时中止，可就绪的就绪 """
        #超时检查,这个要在checkReady前调用
        self.checkTimeOut()
        #就绪检查
        self.checkReady()

    def hadoop_init(self):
        self.hadooprs = ResourceSchedule()             #hadoop连接初始化
        #hadoop任务调度参数,第一个参数为hadoop检查时间，第二个参数为hadoop任务更新运行状况时间
        self.hadpara = {'chtm1':int(time.time() - 150),
                        'chtm2':int(time.time() - 86400)
                        }
        self.hadjobs = []                              #hadoop任务

    def priorityRun(self):
        """ 任务排优先级并开始新进程 """
        tup = sorted(self.readyJobs.iteritems(), key = lambda x:x[1]['pri'], reverse=False)
        self.split_hadoop(tup)
        # 防止 tup 为空
        if  self.hadjobs:
            if int(time.time()) - self.hadpara['chtm1'] > self.hadooprs.get_check_interval():    #距离上次检查时间是否大于90秒
                self.run_hadoop()

    def split_hadoop(self,tup):
        '''分出hadoop任务方便后面批量执行,临时任务都是hadoop任务'''
        # print 'spliting hadoop ...,' + time.strftime('%Y-%m-%d',time.localtime(time.time()))
        self.hadjobs = []
        for t in tup:
            self.hadjobs.append({t[0]:t[1]})

    def run_hadoop(self):
        '''批量执行hadoop任务'''
        # print 'starting hadoop jobs ...,'+ time.strftime('%Y-%m-%d',time.localtime(time.time()))
        self.hadpara['chtm1'] = int(time.time())
        #可执行hadoop任务的jid列表
        av_ctn_list = self.hadooprs.get_need_start_tasks(self.hadjobs)

        st_count = 0
        for key in av_ctn_list:     #开启满足资源数的hadoop任务
            if self.maxhadoop_pn <= self.hadoop_pn + st_count:
                print "最大进程数限制，无法启动tempjob:{}, self.maxhadoop_pn={}, self.hadoop_pn={}".format(key, self.maxhadoop_pn, self.hadoop_pn)
                break
            st_count += 1
            self.startJob(key)

        self.proc_num_update()    #进程数更新

    def close(self):
        """ 关闭数据库和子程序 """
        for k,v in self.record.items():
            v.join()
        self.mdb.close()
        print 'The while tempjob loop is over.---' + time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())

    def doAlljobs(self):
        self.refreshJobs()
        self.joinJob()
        self.checkJobs()
        self.priorityRun()
        self.CleanMode()

    def worker(self, jid):
        """ 任务执行程序 """
        start_mark = '\nstart tempjob proc jid = %s | %s\n' % (jid,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()) )
        print start_mark

        calling = self.jobs[jid]['calling'].strip()
        args = calling.split(' ')    #参数要装成list传递
        args[0] = '%s/jobs/%s' % (os.getenv('SCS_PATH'), args[0])
        args.insert(0, '/usr/local/python272/bin/python')
        args.append(self.jobs[jid]['d_time'])
        args.append('%s' % jid)
        if self.jobs[jid]['debug']:
            args.append('%s' % self.jobs[jid]['debug'])

        # 对所有任务按数据日期分出目录，按任务jid分出日志文件
        path = '/data/other/scs_log/jobslog/dtime_%s' % self.jobs[jid]['d_time']
        if not os.path.exists(path):
            os.makedirs(path)
        joblog = open(path + '/jid_%s.log' %jid,'a+')
        joblog.write(start_mark)
        joblog.flush()      #可以将开始标记立即刷新至开头
        wrongmsg = ''
        try:
            p = subprocess.Popen(args,stdout=joblog, stderr=subprocess.PIPE)

            def dokill(sig, arg):    #处理信号接收
                print 'do kill tempjob processor %s | %s' % (jid, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()) )
                p.send_signal(signal.SIGINT)    #SIGINT可以终止hadoop任务
                print '------------------%s | %s' % (jid, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()) )
                #signal.alarm(60)    #这里加个试试，看能不能防止主进程卡死，最多等待60秒

            signal.signal(signal.SIGTERM, dokill)    #接收信号后调用dokill,signal.SIGTERM是terminate()默认的信号
            p.wait()
            ret = p.returncode
            if 0 != ret:    #返回值不为0就是运行出错了
                self.queerr.put(jid)
                # 将日任务出错信息插入到运行日志
                wrongmsg = p.stderr.read()
            else:
                self.queue.put(jid)
        # 防止启动任务进程时出错
        except Exception, e:
            self.queerr.put(jid)
            wrongmsg = traceback.format_exc()

        # 添任务结束分割线，方便查看日志
        finally:
            end_mark = '\nend tempjob proc jid = %s | %s\n' % (jid,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))
            splitline = '\n*******************************************************************\n'
            msg = end_mark + wrongmsg + splitline
            joblog.write(msg)
            joblog.close()
            print end_mark


if __name__ == '__main__':
    mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )
    Tw = Tempworker(mdb)
    while True:
        ## 获取任务信息,每次都重新获取是兼容人为的一些操作
        Tw.doAlljobs()

    else:
        Tw.close()
