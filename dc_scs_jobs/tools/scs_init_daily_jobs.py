#-*- coding: UTF-8 -*-
import os
import datetime
import time
import sys
from optparse import OptionParser

from sys import path
path.append( os.getenv('DC_SERVER_PATH') )
path.append( os.getenv('SCS_PATH') )
# path.append( 'D:\\Linux_tool\\bydc\\dc_server' )
# path.append( 'D:\\Linux_tool\\bydc\\dc_server\\dc_scs_jobs' )

import config
from libs.DB_Mysql import Connection as m_db
from libs.warning_way import send_sms


class ToEntityJob():
    def __init__(self, m_db):

        self.mdb = m_db(host      = config.DB_HOST,
                        database  = config.DB_NAME,
                        user      = config.DB_USER,
                        password  = config.DB_PSWD)

        self.d_time = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
        self.bind_info = self.mdb.query("SELECT cid, pid FROM `job_bind`")
        self.interval = 60 #  每隔60分钟启动一次实例化脚本，crontab配置的分钟间隔数应该要等于interval

    def backupConfig(self, jconfig, date):
        """ 备份下配置表 """
        file_name = '/data/other/scs_log/config_backup/config_%s.sql' % (date)
        try:
            f = open( file_name, 'a')
            print >> f, jconfig
            print >> '\n'
            f.close()
        except Exception, e:
            pass


    def checkJobcycle(self, time_s, jobcycle):
        """ 检查周期是否匹配  """
        #%M    十进制分钟[00,59]
        mi= time.strftime("%M", time_s)
        #%H    24进制的小时[00,23]
        h = time.strftime("%H", time_s)
        #%d    当月的第几天 [01,31]
        d = time.strftime("%d", time_s)
        #%m    十进制月份[01,12]
        m = time.strftime("%m", time_s)
        #%w    十进制的数字，代表周几 ;0是周日，1是周一
        w = time.strftime("%w", time_s)

        # 假定任务度可以实例化，不满足日期格式就不实例化
        flag = 1
        cycle = jobcycle.split(' ')
        if 5 != len(cycle):
            return 0

        # 先从大粒度时间开始检查
        if cycle[2] != '*' :
            if int(cycle[2]) != int(d):
                return 0

        if cycle[3] != '*':
            if int(cycle[3]) != int(m):
                return 0

        if cycle[4] != '*':
            if int(cycle[4]) != int(w):
                return 0

        if cycle[1] != '*' and '/' not in cycle[0]:
            start = int(cycle[1]) * 60 + int(cycle[0])
            # 如果实例化时间处于配置时间段及间隔interval这个区间，就实例化该任务
            if  start <= int(h)*60 + int(mi) < start + self.interval :
                flag = 1
            else:
                flag = 0

        # 小时级任务检查
        if '/' in cycle[0] :
            minute = int(cycle[0].replace('/',''))
            start = int(cycle[1]) * 60

            if (int(h)*60 + int(mi) - start) % minute ==0 and \
            int(h)*60 + int(mi) >= start:

                flag = 1
            else:
                flag = 0

        return flag

    def dailyJobs(self, date, bind_info):
        """ 实例化任务并插入实体数据表 """
        #jobcycle 分　时　日　月　周,暂时不处理分级别的任务
        now = time.localtime()
        snums = 0
        nums = 0
        # 每天00点初始化日常任务
        h = time.strftime("%H", now)    #%H    24进制的小时[00,23]

        jconfig = self.mdb.query("SELECT * FROM `jconfig` WHERE `open` = 1")

        backuplist = []

        for row in jconfig:
            #按照任务周期,把符合周期条件的任务都先实例化出来
            if not self.checkJobcycle(now, row['jobcycle']):
                continue

            snums += 1

            in_entity_sql = """INSERT INTO `job_entity` (`entity_time`, `start_time`, `end_time`, `jid`, `d_time`, `status`,`entity_type`)
                   VALUES (unix_timestamp(now()), 0, 0, %s, '%s', 1, 11)
                   ON DUPLICATE KEY UPDATE `jid` = %s """ % (row['jid'], date, row['jid'])
            try :
                print(in_entity_sql)
                self.mdb.execute(in_entity_sql)
                nums += 1
            except Exception as e:
                print('insert into job_entity failed.')
                send_sms(['HymanShi'], '调度系统实例化每日任务失败!', 8)
                pass

            backuplist.append(in_entity_sql)

        if 0==snums:
            return nums

        self.backupConfig(backuplist, date)
        return nums


    #初始化日常任务
    def dailyJobs_bak(self, date, bind_info):
        """ 实例化任务并插入实体数据表 """
        #jobcycle 分　时　日　月　周,暂时不处理分级别的任务
        now = time.localtime()
        snums = 0
        nums = 0
        # 每天00点初始化日常任务
        h = time.strftime("%H", now)    #%H    24进制的小时[00,23]

        jconfig = self.mdb.query("SELECT * FROM `jconfig` WHERE `open` = 1")


        backuplist = []
        in_entity = 'INSERT INTO `job_entity` (`entity_time`, `start_time`, `end_time`, `jid`, `d_time`, `status`,`entity_type`) VALUES '

        for row in jconfig:
            #按照任务周期,把符合周期条件的任务都先实例化出来
            if not self.checkJobcycle(now, row['jobcycle']):
                continue

            snums += 1

            in_entity += """(unix_timestamp(now()), 0, 0, %s, '%s', 1, 11),""" % (row['jid'], date)
            backuplist.append({'jid':row['jid'], 'entity_time':time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()),'jobcycle':row['jobcycle'] })

        if 0==snums:
            return nums

        self.backupConfig(backuplist, date)
        in_entity = in_entity[:-1]    #去掉最后的逗号
        #print in_entity
        nums = self.mdb.execute_rowcount(in_entity)

        return nums


if __name__ == '__main__':
    TEJ = ToEntityJob(m_db)
    TEJ.dailyJobs(TEJ.d_time, TEJ.bind_info)

##END
