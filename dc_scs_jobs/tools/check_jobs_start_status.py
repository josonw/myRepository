#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
@description: 周期性任务启动状态检查
@author: HymanShi <HymanShi@boyaa.com>
@datetime: 2016-11-16 13:45:39
"""

import os
import sys
import datetime
import time
# 把 str 编码由 ascii 改为 utf8 (或 gb18030)
reload(sys)
sys.setdefaultencoding("utf-8")


dc_path = os.getenv('DC_SERVER_PATH')
from sys import path
path.append(dc_path)

from dc_scs_jobs import config
from libs.DB_Mysql import Connection as m_db
from libs.warning_way import send_sms


def get_members(mdb, is_time, sort_item=None):
    """ 找出相应配置的人员，并排好序"""
    sql = 'SELECT * FROM loop_member where isworktime >= %s' % is_time
    data = mdb.query(sql)
    if sort_item == None:
        sort_item = 'priority'
    temp = sorted(data, key=lambda x: x[sort_item], reverse=False)
    result = [item['user'] for item in temp]
    return result


def is_worktime():
    """ 判断当前时间是否是工作时间 """
    h = int(time.strftime("%H", time.localtime()))
    # d = int(time.strftime("%w", time.localtime()))
    if 9 <= h <= 24:
        return 1
    else:
        return 0


def isUp2JobRunTime(jobcycle):
    now = time.localtime()

    """ 检查周期是否匹配  """
    #%M    十进制分钟[00,59]
    mi = time.strftime("%M", now)
    #%H    24进制的小时[00,23]
    h = time.strftime("%H", now)
    #%d    当月的第几天 [01,31]
    d = time.strftime("%d", now)
    #%m    十进制月份[01,12]
    m = time.strftime("%m", now)
    #%w    十进制的数字，代表周几 ;0是周日，1是周一
    w = time.strftime("%w", now)

    cron_list = jobcycle.split(' ')
    if len(cron_list) != 5:
        return False

    cron_minute, cron_hour, cron_day, cron_month, cron_week = cron_list

    # 先从大粒度时间开始检查
    if (cron_day != '*') and (int(cron_day) != int(d)):
        return False

    if (cron_month != '*') and (int(cron_month) != int(m)):
        return False

    if (cron_week != '*') and (int(cron_week) != int(w)):
        return False

    if (cron_hour != '*') and ('/' not in cron_minute):
        # 任务计划运行时间
        plan_time = int(cron_hour) * 60 + int(cron_minute)

        # 从任务计划运行时间10分钟后开始检查
        if (plan_time + 10) <= int(h) * 60 + int(mi):
            # 比如一个任务30 01 * * * 计划在1:30启动，
            # 1:40开始检查任务是否启动
            # 并且只在当前小时内检查
            if (int(h) == int(cron_hour)):
                return True
            else:
                return False
        else:
            return False

    # 小时级任务检查
    if '/' in cron_minute:
        minute = int(cron_minute.replace('/', ''))
        plan_time = int(cron_hour) * 60

        if ((int(h) * 60 + int(mi) - plan_time) % minute == 0) and (int(h) * 60 + int(mi)) >= plan_time:
            return True
        else:
            return False


def checkJobStartStatus(mdb):
    print('start to check job status...')
    print("current time: %s" % datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d %H:%M:%S"))
    dtime = datetime.datetime.strftime(
        datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d")

    dtime_now = datetime.datetime.strftime(
        datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")

    sql = """ SELECT * FROM `jconfig` WHERE `open` = 1 """
    jobs = mdb.query(sql)

    error_list = []

    for job in jobs:
        jobcycle = job['jobcycle']
        jid = job['jid']

        if not isUp2JobRunTime(jobcycle):
            # 未到任务计划运行时间
            continue

        # 检查此任务当天是否运行过
        chsql = """SELECT * FROM `job_entity` WHERE `jid` = %d AND `d_time` = '%s'""" % (
            jid, dtime)
        qdata = mdb.query(chsql)

        if not len(qdata):
            # 当天任务运行检查不存在
            njob = (job['calling'], job['jobcycle'], job['jobdesc'])
            error_list.append(njob)

    # 有未启动的任务
    if len(error_list):
        #sms_user = get_members(mdb, is_worktime())
        # if not len(sms_user):
        sms_user = ['HymanShi']
	print(sms_user)

        for errjob in error_list:
            error_msg = '任务未启动告警：任务 %s 未启动，当前时间：%s, 计划启动时间：%s, 任务说明：%s' % (
                errjob[0], dtime_now, errjob[1], errjob[2])
	    print(error_msg)
            send_sms(sms_user, error_msg, 8)
    print('check job status finished...')


if __name__ == '__main__':
    # 数据库链接
    mdb = m_db(config.DB_HOST, config.DB_NAME,
               user=config.DB_USER, password=config.DB_PSWD)
    checkJobStartStatus(mdb)
    mdb.close()
