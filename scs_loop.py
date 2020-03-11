#-*- coding: UTF-8 -*-

import os
import time
import traceback
import sys

from libs.DB_Mysql import Connection as m_db
from dc_scs_jobs.engine import Mworker
from dc_scs_jobs.engine_tjob import Tempworker
from libs.warning_way import send_sms
# path.append('%s/' % os.getenv('SCS_PATH'))
from dc_scs_jobs import config
import datetime
# import config

def run(mdb):
    """ 循环调度 """
    ##初始化多任务类##多进程跑存储过程
    mw = Mworker(mdb)
    # 多进程跑业务临时查询任务
    Tw = Tempworker(mdb)
    loop = True
    print 'The while loop is start---'+time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    while loop:
        ## 获取动态配置项
        mw.refreshConfig()

        ## 获取任务信息,每次都重新获取是兼容人为的一些操作
        mw.refreshJobs()

        ## 检查并回收完成的子进程
        mw.joinJob()

        ## 任务检查，超时就中止掉,可就绪的就绪
        mw.checkJobs()

        ## 排优先级并开始新进程
        mw.priorityRun()

        if 0 == mw.gethadooppn() and 0 == mw.getotherpn():
            time.sleep(1)
            mw.casualMode()       #没有任务进程时会把所有出错任务再重启下
        # 调试用
        try:
            Tw.doAlljobs()
        except Exception, e:
            wrongmsg = traceback.format_exc()
            print wrongmsg
    else:
        mw.close()

def record_err(wrongmsg):
    # 记录主进程错误信息
    start_mark = '\nloop run error | %s\n' % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))
    splitline = '\n*******************************************************************\n'
    f = open('/data/other/scs_log/run_loop_err.log','a+')
    if wrongmsg:
        f.write(start_mark + wrongmsg + splitline)
    else:
        f.write(start_mark + 'no error message' + splitline)
    f.close

def kill_allsubprocess():
    # 主进程出错时杀死所有子进程,主进程才能自动重启
    cmd0 = 'ps -ef | grep python272 | grep hadoop | grep -v 127.0 | grep -v grep | grep -v supervisord '
    cmd1 = cmd0 + " | awk '{print $2}' | xargs kill -9 "
    temp = os.popen(cmd0).read()
    print '\nloop drop out ,The following process will be Terminated | %s\n' \
          % (time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())),temp

    result = os.popen(cmd1).read()
    print result
    sys.exit(1)

def main():

    #数据库链接
    mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )
    admin = mdb.query("select user from loop_member where priority=1")
    admin = [item.get('user','') for item in admin]
    hour = int(datetime.datetime.now().hour)
    if (hour>=19 and hour<=24) or (hour<9):  #非上班时间，才打电话
        send_sms(admin,'调度系统正在重启', 8)
    ## 开始执行任务
    while True:
        try:
            run(mdb)
        except Exception, e:
            wrongmsg = traceback.format_exc()
            if wrongmsg:
                record_err(wrongmsg)
            send_sms(user_list=admin, content="调度系统出现异常，详情查看log;/data/other/scs_log/run_loop_err.log", priority=3)
            # kill_allsubprocess()

# Main
if __name__ == '__main__':
    # 用shell检测当前进程中有无loop任务已经启动
    print 'scs_loop.py is starting'
    main()
    print 'scs_loop.py is over'
####END
