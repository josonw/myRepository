#-*- coding: UTF-8 -*-
import time
import multiprocessing
import os
import sys
import signal
import subprocess
import traceback
from itertools import chain

from libs.warning_way import send_sms
import BaseStat
from ResourceSchedule import ResourceSchedule
from job_bind_info import getkids_RecurTree

reload(sys)
sys.setdefaultencoding( "utf-8" )



class Mworker:
    """ 多进程调度类 """
    def __init__(self, mdb):
        self.maxhadoop_pn = 40                             #最大hadoop任务数
        self.maxother_pn = 15                             #最大其他任务数
        self.hadoop_pn = 0                             #hadoop任务数
        self.other_pn = 0                              #其他任务数

        self.isAlarm = 1                            #是否开启告警 1开启 0关闭

        self.queue = multiprocessing.Queue()        #进程间通信用 记录完成任务的eid
        self.queerr = multiprocessing.Queue()        #存储脚本执行异常的任务
        self.mdb = mdb
        self.record = {}                            #存储进程信息
        self.jobs = {}                                #存储待处理的任务信息
        self.readyJobs = {}                            #存储已就绪的任務
        self.hadoop_init()

    def refreshConfig(self):
        """ 获取动态配置 """
        data = self.mdb.query("SELECT config_name, config_value FROM `loop_config`")
        for row in data:
            if row.get('config_name') and row.get('config_value'):
                if 'maxhadoop_pn' == row['config_name']:
                    if row['config_value'].isdigit():
                        self.maxhadoop_pn = int(row['config_value'])
                if 'maxother_pn' == row['config_name']:
                    if row['config_value'].isdigit():
                        self.maxother_pn = int(row['config_value'])
                if 'isAlarm' == row['config_name']:
                    if row['config_value'].isdigit():
                        self.isAlarm = int(row['config_value'])
                if 'update_task_size' == row['config_name']:
                    if row['config_value']:
                        update_arr = row['config_value'].split("&")
                        if "1" == update_arr[0]:
                            if 3 == len(update_arr):
                                self.hadooprs.update_hadoop_info(\
                                    time.mktime(time.strptime(update_arr[1], "%Y%m%d%H%M%S")), \
                                    time.mktime(time.strptime(update_arr[2], "%Y%m%d%H%M%S")))
                            else:
                                self.hadooprs.update_hadoop_info()

                            self.mdb.execute("""update loop_config set config_value='0' where config_name='update_task_size'""");
            else:
                print 'mysql select from byscs.loop_config error:'
                print row

    def refreshJobs(self):
        """ 刷新任务信息 """
        self.jobs = {}
        self.readyJobs = {}
        sql = """SELECT eid, jid, pjid, queue, running_time, pri, calling, d_time,
                 maxtime, rerun, status, u_master FROM `job_running`
              """
        data = self.mdb.query(sql)
        for row in data:
            if row.get('eid'):
                self.jobs[ row.get('eid') ] = row
            else:
                print 'mysql select from byscs.job_running error:'
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

    def getallkids_num(self, jid):
        """ 取得某个任务的所有子任务或父任务数接口 """
        ids = [jid]
        bind_info = self.mdb.query("SELECT cid, pid FROM `job_bind`")
        bind_tree = [[x['cid'],x['pid']] for x in bind_info]
        allkids_list = getkids_RecurTree(ids, bind_tree, 1)

        return len(allkids_list)


    def ishadooptask(self,eid):
        """ 由eid来判断是否是hadoop任务，区别于 self.hadooprs.is_hadoop_task(jid)"""
        sql = """SELECT eid,jid FROM `job_entity` WHERE eid = %s""" % eid
        data = self.mdb.getOne(sql)
        flag = 0
        if not data :
            print 'update proccess numbers fail, eid=%s | %s' %(eid,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()) )
        elif self.hadooprs.is_hadoop_task(data['jid']):
            flag = 1

        return flag

    def proc_num_update(self):
        """进程数根据mode,num,eid发生相应更新"""

        sql = "select count(*) as run_num from job_running as jr left join jconfig as j on jr.jid=j.jid where status=3 and j.jobtype not regexp '[同步]' "
        data_dict = self.mdb.getOne(sql)
        self.hadoop_pn = data_dict.get('run_num', 0)
        sql = "select count(*) as run_num from job_running as jr left join jconfig as j on jr.jid=j.jid where status=3 and j.jobtype regexp '[同步]' "
        data_dict = self.mdb.getOne(sql)
        self.other_pn = data_dict.get('run_num', 0)

        proc_info = """The current hadoop jobs nums: %s, other jobs nums: %s | %s""" \
                    %(self.hadoop_pn,self.other_pn,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))
        print proc_info

    def getReadyJobs(self):
        """ 获取就绪的任务信息 """
        return self.readyJobs

    def get_jidconfig(self, jid, field, *args):
        """ 获取任务的相关配置信息 """
        sql = """SELECT jid, calling, jobmark, jobtype, jobdesc, tab_from, tab_into, pri, jobcycle,
                 maxtime, maxrerun, regap, alarm, queue, update_time, open, u_master, u_concern
                 FROM `jconfig` WHERE jid = %s""" % jid
        data = self.mdb.getOne(sql)
        if data:
            if not args:
                return data[field]
            else:
                return data
        else:
            return None

    def get_detail(self, eid):
        """ 获取明细表中的最近的did entity_time"""
        sql = """SELECT did, eid, jid, entity_time, start_time, end_time, d_time, start_type, end_type
                 FROM `job_detail` WHERE eid = %s ORDER BY did DESC LIMIT 1""" % eid

        data = self.mdb.getOne(sql)
        if not data:
            return False
        return data

    def insert_detail(self, eids, start_type=1, entity_time=None):
        """ 同步并插入明细表，一个任务的就绪准备 """
        #start_type'启动方式： 11：凌晨任务自启；12：超时后自启；13：出错后自启；4：页面启动',
        if isinstance(eids, list):
            eids_list = ','.join([str(item) for item in eids])
        else:
            eids_list = str(eids)

        en_sql = """ SELECT eid, jid, entity_time, d_time FROM job_entity WHERE eid in (%s) """ % eids_list
        data = self.mdb.query(en_sql)

        for row in data:
            row['start_type'] = start_type
            if entity_time:
                row['entity_time'] = entity_time

        sql = """ INSERT INTO `job_detail` (`eid`,`jid`,`entity_time`, `start_time`, `end_time`, `d_time`, `start_type`,`end_type`)
                   VALUES  (%(eid)s, %(jid)s, %(entity_time)s, 0, 0, %(d_time)s, %(start_type)s, 0) """
        self.mdb.executemany_rowcount(sql, data)

    def update_detail(self, did, start_time):
        """ 同步更新明细表，更新一个任务的开启时间 """
        sql = """ UPDATE `job_detail` SET start_time = %s WHERE did = %s""" %( start_time, did)
        self.mdb.execute(sql)

    def overjob_detail(self, did, end_time, end_type=6):
        """ 同步更新明细表，一个任务的结束"""
        #end_type  '结束方式：4：超时；5：出错；6：完成；7：停止'  值和job_running表同步
        sql = """ UPDATE `job_detail` SET end_time = %s, end_type = %s WHERE did = %s""" %( end_time, end_type, did)
        self.mdb.execute(sql)

    def casualMode(self):
        """开启休闲模式"""
        #更新hadoop任务运行记录
        h = time.strftime("%H", time.localtime())
        tm = int(time.time())
        # 每晚九点以后，距离上次更新超过一天时间
        if int(h) >= 22 and tm - self.hadpara['chtm2'] >= 9000:
            # 确保每一天开始时的调度策略都是DFR
            self.mdb.execute("update loop_config set config_value='DFR' where config_name='schedule_policy' ")

            today_str = time.strftime("%Y-%m-%d", time.localtime())
            # 每晚九点以后更新当天的hadoop任务运行记录
            start_str = today_str + ' 00:00:00'
            start_stamp = time.mktime(time.strptime(start_str, "%Y-%m-%d %H:%M:%S"))   #转换成时间戳
            end_str = today_str + ' 21:00:00'
            end_stamp = time.mktime(time.strptime(end_str, "%Y-%m-%d %H:%M:%S"))
            two_month = start_stamp - 86400 * 60
            # jid中在最近两月的记录中，按实例化时间汇总累计总次数为1 的说明是今天新增的任务(某些月任务两三月跑一次的，可能会包含)
            sql = """SELECT  jid ,max(etime) max_etime, count(jid) cn FROM
                    (SELECT jid ,FROM_UNIXTIME(entity_time, '%%%%Y-%%%%m-%%%%d') etime
                    FROM job_entity
                    WHERE entity_time > %s AND status = 6
                    GROUP BY jid, FROM_UNIXTIME(entity_time, '%%%%Y-%%%%m-%%%%d')
                    ) b
                    GROUP BY jid HAVING cn =1 and max(etime) = '%s'
                    """ % (two_month,today_str)

            data = self.mdb.query(sql)
            if data:
                for item in data:
                    self.hadooprs.save_new_task(item['jid'])
                print 'save new hadoop job running log success | %s'%(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()) )

            data = self.hadooprs.update_hadoop_info(start_stamp, end_stamp)
            if data:
                self.hadpara['chtm2'] = int(time.time())                                   #重新刷新时间
                print 'update hadoop job running log success | %s'%(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()) )

        #如果没有可以跑的任务在跑了，把所有20分钟前出错任务(非人工启动的)再重启下 最大重启到7次
        r_time = int(time.time()) - 1200 # 加个时间，否则调试的时候一直重启

        sql = """SELECT d.eid,d.start_type,d.end_type FROM
                (SELECT did,eid ,start_type,end_type FROM job_detail
                WHERE start_type=11 AND end_type=5 AND start_time <%s ORDER BY did DESC) d
                INNER JOIN
                (SELECT eid FROM job_running WHERE status = 5 AND rerun <= 6 AND running_time <%s) r
                ON d.eid=r.eid
                GROUP by r.eid""" % (r_time,r_time)

        data = self.mdb.query(sql)

        if data:
            eids = [str(item['eid']) for item in data]
            eidsstr = ','.join(eids)
            up_sql = """ UPDATE `job_running` SET status = 2, rerun = rerun + 1 WHERE eid in (%s)""" %eidsstr
            self.mdb.execute(up_sql)
            self.insert_detail(eids, 13)   #同步更新至job_detail

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
            print 'job err_log mysqldb insert %s, eid is %s| %s'%(flag, eid, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()) )

    def doAlarm(self, eid, msg):
        """ 封装告警发送 """
        if 0 == self.isAlarm:
            return False

        info = self.jobs.get(eid, {'jid':0})  #容错
        if 0 == info['jid']:
            return False

        con_sql = """SELECT maxrerun, alarm, u_master, u_concern FROM `jconfig` WHERE jid = %s""" % info['jid']    #获取对应任务的告警级别
        conf_data = self.mdb.getOne(con_sql)

        if 1 == conf_data['alarm']:      #暂时只有短信告警
            return False

        detaildata = self.get_detail(eid)   #非自启的任务，出错不报警
        if detaildata:
            if detaildata['start_type'] >=21:
                return False

        getmember = self.mdb.query("SELECT user, priority, isworktime, period, threshold FROM `loop_member`")
        members = [item['user'] for item in getmember]

        users = []
        if conf_data['u_master'] in members:
            users.append(str(conf_data['u_master']) )

        concern = conf_data['u_concern'].split(',')
        for v in concern:
            if v in members:
                users.append(str(v) )
        #u_master, u_concern字段中没有相关人员就给管理员发送告警
        if 0 == len(users):
            users = [item['user'] for item in getmember if item['priority']==1 ]
        # 如果重跑次数达到最大重启次数且子任务数有10个以上时将会电话告警否则普通短信告警
        num = self.getallkids_num(info['jid'])
        if self.jobs.get(eid,{'rerun' ,0})['rerun'] >= conf_data['maxrerun'] and num >= 10:
            priority = 8
        else:
            priority = 3
        send_sms(users, msg, priority)
        # print 'priority is %s,num is %s' %(priority,num)
        return True


    def startJob(self, eid):
        """ 开启新的子进程 """
        process = multiprocessing.Process(target=self.worker,args=(eid,))
        process.start()
        self.record[eid] = process
        ent_sql = """UPDATE `job_entity` SET status=3, start_time=%s WHERE eid= %s""" % (int(time.time()),eid)
        self.mdb.execute(ent_sql)
        run_sql = """UPDATE `job_running` SET status=3, running_time=%s WHERE eid= %s""" % (int(time.time()),eid)
        self.mdb.execute(run_sql)

        detaildata = self.get_detail(eid)
        if detaildata:
            self.update_detail(detaildata['did'], int(time.time()))

    def joinJob(self):
        """ 回收正常完成的子进程 """
        if 0 < self.getQueueSize():
            eid = self.queue.get()
            if self.record.get(eid, 0) != 0:    #容错
                self.record[eid].join()
                self.proc_num_update()    #进程数更新

                detaildata = self.get_detail(eid)
                if detaildata:
                    self.overjob_detail(detaildata['did'], int(time.time()), 6)

                self.mdb.execute('UPDATE `job_entity` SET status=6, end_time=%s WHERE eid= %s' % (int(time.time()),eid) )

                self.mdb.execute('DELETE FROM `job_running` WHERE eid= %s' % eid)

                del self.record[eid]            #注销进程信息
                del self.jobs[eid]

        if 0 < self.queerr.qsize():
            eid = self.queerr.get()
            if self.record.get(eid, 0) != 0:    #容错
                self.record[eid].join()
                self.proc_num_update()    #进程数更新

                warning_msg = '【调度系统】任务出错：%s' % self.jobs.get(eid,{'calling':'eid-%s'%eid})['calling']
                self.doAlarm(eid, warning_msg)

                detaildata = self.get_detail(eid)
                if detaildata:
                    self.overjob_detail(detaildata['did'], int(time.time()), 5)

                self.mdb.execute('UPDATE `job_running` SET status=5, running_time=%s WHERE eid= %s' % (int(time.time()),eid) )
                del self.record[eid]            #注销进程信息
                if self.jobs.get(eid):
                    self.jobs[eid]['status'] = 5

    def handle_timeout(self, eid):
        self.proc_num_update()    #进程数更新

        detaildata = self.get_detail(eid)
        if detaildata:
            self.overjob_detail(detaildata['did'], int(time.time()), 4)

        maxrerun = self.get_jidconfig(self.jobs[eid]['jid'], 'maxrerun')
        rerun = self.jobs[eid]['rerun']          #看下重启次数
        if rerun >= maxrerun:    #重跑超过次数
            out_sql = """UPDATE `job_running` SET status=5 WHERE eid= %s """ %eid
            self.jobs[eid]['status'] = 5
            #下面发告警
            warning_msg = '【调度系统】任务重启超阀：%s' % self.jobs[eid]['calling']
            self.doAlarm(eid, warning_msg)
            print 'rerun out maxreun eid= %s' % eid
        else:
            out_sql = """UPDATE `job_running` SET status=4, running_time = %s WHERE eid= %s """ % (int(time.time()), eid)
            self.jobs[eid]['running_time'] = int(time.time())
            self.jobs[eid]['status'] = 4
            #下面发告警
            warning_msg = '【调度系统】任务超时：%s' % self.jobs[eid]['calling']
            # self.doAlarm(eid, warning_msg)
            print 'run out maxtime eid= %s' % eid
        self.mdb.execute(out_sql)


    def checkTimeOut(self):
        """ 超时检查，超时的任务中止并回收 """
        temp = []
        for eid,val in self.record.items():
            if not self.jobs.get(eid):
                # 防止出现keyerr 错误
                continue

            out_time = int(time.time()) - self.jobs[eid]['running_time']
            if out_time > self.get_jidconfig(self.jobs[eid]['jid'], 'maxtime') + 600:
                # 超时10分钟后将被强杀,连带子任务进程
                cmd0 = 'ps -ef | grep python272 | grep hadoop | grep -v 127.0 | grep -v grep | grep -v supervisord |grep %s' %val.pid
                cmd1 = cmd0 + " | awk '{print $2}' | xargs kill -9 "
                record_killed = os.popen(cmd0).read()
                print '\nThe following process will be forced killed | %s\n' \
                      % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())),record_killed
                result = os.popen(cmd1).read()
                print result
                temp.append(eid)    #临时表上
                self.handle_timeout(eid)

            elif out_time > self.get_jidconfig(self.jobs[eid]['jid'], 'maxtime'):
                val.terminate()    #退出超时的子进程
                val.join(0.5)         #限时一秒回收子进程，防止阻塞主进程
                if val.is_alive():   #如果没有回收掉该进程继续判断下一个
                    print 'subproc pid: %s, eid: %s run out maxtime can not stop by join' %(val.pid, eid)
                    continue
                temp.append(eid)    #临时表上
                self.handle_timeout(eid)

        #进程信息中删除已重启的
        for eid in temp:
            del self.record[eid]
            self.writeLog(eid, '任务超时！')

    def checkReady(self):
        """ 就绪检查，修改任务状态：1：挂起；2：就绪；3：运行；4：超时；5：出错；6：完成；7：停止；8：待启动 """
        temp = []    #已完成的先装进来，循环完后删除
        for eid,val in self.jobs.iteritems():
            # 防止出现 KeyErr 的错误
            if not (val.get('jid') and val.get('status') and val.get('d_time')):
                continue

            if 8 == val['status']:  #待启动的任务 不用任何处理
                continue

            #停止的任务，如果进程开启了就杀掉，没开进程的跳过
            if 7 == val['status']:
                if self.record.get(eid, 0) != 0:
                    self.record[eid].terminate()    #杀掉子进程
                    self.record[eid].join(0.5)         #限时一秒停止子进程，防止阻塞主进程
                    if self.record[eid].is_alive():   #如果这次没有回收掉该进程，继续判断下一个
                        print 'subproc pid: %s, eid: %s can not stop by join' %(self.record[eid].pid, eid)
                        continue
                    self.proc_num_update()    #进程数更新
                    detaildata = self.get_detail(eid)
                    if detaildata:
                        self.overjob_detail(detaildata['did'], int(time.time()), 7)
                    del self.record[eid]    #进程信息中删除
                continue

            #已完成的清理掉
            if 6 == val['status']:
                sql = """UPDATE `job_entity` SET status = 6, end_time=%s WHERE eid = %s""" % (int(time.time()),eid)
                self.mdb.execute(sql)
                self.mdb.execute('DELETE FROM `job_running` WHERE eid= %s' % eid)
                temp.append(eid)

            #出错的任务重启
            if 5 == val['status']:
                regap = self.get_jidconfig(val['jid'], 'regap')
                if (int(time.time()) - val['running_time']) < regap:
                    continue
                rerun = self.jobs[eid]['rerun']         #看下重启次数
                if rerun >= self.get_jidconfig(val['jid'], 'maxrerun'):
                    continue

                detaildata = self.get_detail(eid)   #人工重启的任务出错后不自启
                if detaildata:
                    if detaildata['start_type'] >=21:
                        continue

                #已经过了缓冲时间并且小于重启次数，改成就绪状态,重跑
                self.jobs[eid]['status'] = 2
                sql = """UPDATE `job_running` SET status = 2, rerun = rerun + 1  WHERE eid = %s""" % eid
                self.insert_detail(eid, 13)
                self.mdb.execute(sql)

            #超时过的任务比较特殊，最少等上次结束x分钟后再跑
            if 4 == val['status']:
                regap = self.get_jidconfig(val['jid'], 'regap')
                if (int(time.time()) - val['running_time']) < regap:
                    continue
                else:    #已经过了缓冲时间，改成就绪状态
                    self.jobs[eid]['rerun'] += 1
                    self.jobs[eid]['status'] = 2
                    sql = """UPDATE `job_running` SET status = 2, rerun = rerun + 1  WHERE eid = %s""" % eid
                    self.insert_detail(eid, 12)
                    self.mdb.execute(sql)

            #运行中的检查下是否与进程信息匹配, 不匹配的改成出错
            if 3 == val['status']:
                if '' == self.record.get(eid, ''):
                    self.mdb.execute("""UPDATE `job_running` SET status = 5 WHERE eid = %s""" % eid)
                    self.writeLog(eid, 'loop进程 任务调度出错！')
                else:  #更新hadoop任务进度
                    hadoopsch = self.hadooprs.get_task_progress(eid)
                    self.mdb.execute("""UPDATE `job_running` SET hadoop_sch = %s WHERE eid = %s""" %(hadoopsch['vcores'], eid))

            #本来就是已就绪的
            if 2 == val['status']:
                self.readyJobs[eid] = val

            if 1 == val['status']:    #判断父任务都有木有完成，任务依赖检查以转移至check_pjob_status.py检查
                self.jobs[eid]['status'] = 2
                sql = """UPDATE `job_running` SET status = 2  WHERE eid = %s""" % eid
                self.mdb.execute(sql)

        #删除已完成的任务
        for eid in temp:
            del self.jobs[eid]


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
            if int(time.time()) - self.hadpara['chtm1'] > self.hadooprs.get_check_interval():    #距离上次检查时间是否大于指定间隔时间
                self.run_hadoop()

        for t in tup:
            if self.hadooprs.is_hadoop_task(t[1]['jid']):
                continue
            elif self.maxother_pn > self.other_pn:
                self.startJob(t[0])
                self.proc_num_update()    #进程数更新
            else:
                print "最大进程数限制，无法启动other job:{}, self.maxother_pn={}, self.other_pn={}".format(t[0], self.maxother_pn, self.other_pn)
                break

    def split_hadoop(self,tup):
        '''分出hadoop任务方便后面批量执行'''
        # print 'spliting hadoop ...,' + time.strftime('%Y-%m-%d',time.localtime(time.time()))
        self.hadjobs = []
        for t in tup:
            if self.hadooprs.is_hadoop_task(t[1]['jid']):
                self.hadjobs.append({t[0]:t[1]})

    def run_hadoop(self):
        '''批量执行hadoop任务'''
        # print 'starting hadoop jobs ...,'+ time.strftime('%Y-%m-%d',time.localtime(time.time()))
        self.hadpara['chtm1'] = int(time.time())
        #可执行hadoop任务的eid列表
        av_ctn_list = self.hadooprs.get_need_start_tasks(self.hadjobs)

        st_count = 0
        for key in av_ctn_list:     #开启满足资源数的hadoop任务
            if self.maxhadoop_pn <= self.hadoop_pn + st_count:
                print "最大进程数限制，无法启动hadoop job:{}, self.maxhadoop_pn={}, self.hadoop_pn={}".format(key, self.maxhadoop_pn, self.hadoop_pn)
                break
            else:
                # st_count += 1
                self.startJob(key)
                self.proc_num_update()    #进程数更新



    def close(self):
        """ 关闭数据库和子程序 """
        for k,v in self.record.items():
            v.join()
        self.mdb.close()
        print 'The while loop is over.---' + time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())

    def worker(self, eid):
        """ 任务执行程序 """
        start_mark = '\nstart proc eid = %s | %s\n' % (eid,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()) )
        print start_mark

        calling = self.jobs[eid]['calling'].strip()
        args = calling.split(' ')    #参数要装成list传递
        args[0] = '%s/jobs/%s' % (os.getenv('SCS_PATH'), args[0])
        args.insert(0, '/usr/local/python272/bin/python')
        args.append(self.jobs[eid]['d_time'])
        args.append('%s' % eid)

        # 对所有任务按数据日期分出目录，按任务jid分出日志文件
        path = '/data/other/scs_log/jobslog/dtime_%s' % self.jobs[eid]['d_time']
        if not os.path.exists(path):
            os.makedirs(path)
        joblog = open(path + '/jid_%s.log' %self.jobs[eid]['jid'] ,'a+')
        joblog.write(start_mark)
        joblog.flush()      #可以将开始标记立即刷新至开头
        wrongmsg = ''
        try:
            p = subprocess.Popen(args,stdout=joblog, stderr=subprocess.PIPE)

            def dokill(sig, arg):    #处理信号接收
                print 'do kill processor %s | %s' % (eid, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()) )
                p.send_signal(signal.SIGINT)    #SIGINT可以终止hadoop任务
                print '------------------%s | %s' % (eid, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()) )
                #signal.alarm(60)    #这里加个试试，看能不能防止主进程卡死，最多等待60秒

            signal.signal(signal.SIGTERM, dokill)    #接收信号后调用dokill,signal.SIGTERM是terminate()默认的信号
            p.wait()
            ret = p.returncode
            if 0 != ret:    #返回值不为0就是运行出错了
                self.queerr.put(eid)
                # 将日任务出错信息插入到运行日志
                wrongmsg = p.stderr.read()
            else:
                self.queue.put(eid)
        # 防止启动任务进程时出错
        except Exception, e:
            self.queerr.put(eid)
            wrongmsg = traceback.format_exc()

        # 添任务结束分割线，方便查看日志
        finally:
            end_mark = '\nend proc eid = %s | %s\n' % (eid,time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))
            splitline = '\n*******************************************************************\n'
            msg = end_mark + wrongmsg + splitline
            joblog.write(msg)
            joblog.close()
            print end_mark
