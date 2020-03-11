#-*- coding: UTF-8 -*-
"""
创建于2015-10-12
@作者:陈军1654
@简介：监控集群任务
"""
import re
import os
import time
from libs.DB_Mysql import Connection as m_db
from dc_scs_jobs.RTWorker import RTWorker
from sys import path
from dc_scs_jobs import config


"""
监控实时任务的运行状况，失败时发送告警
"""
def monitor_RT_task(mdb):
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),"Begin to monitor real time tasks..."
    
    """创建实时任务管理类"""
    RT_worker = RTWorker(mdb)

    while True:
        RT_worker.check()
        time.sleep(1800)


def main():

    #数据库链接
    mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )

    """
    启动实时任务监控
    """
    try:
        monitor_RT_task(mdb)
    except Exception, e:
        print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),"Fail to start real time tasks monitor: ",str(e)
        raise e


# Main
if __name__ == '__main__':

    cmd='ps -ef | grep "task_monitor.py"'
    a=os.popen(cmd).read()
    pattern=re.compile(r'task_monitor.py')
    result=pattern.findall(a)
    if len(result)==0:
        print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),'task_monitor.py is not running')
    elif len(result) ==3 :
        print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),'task_monitor.py is starting'
        main()
    elif len(result) > 3 :
        print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),'task_monitor.py is running'
##END