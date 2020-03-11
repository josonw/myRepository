#-*- coding: UTF-8 -*-
import time
import sys
import os
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
import config
from libs.DB_Mysql import Connection as m_db
from libs.warning_way import send_sms

"""检查调度系统任务依赖是否有死循环"""

mdb = m_db(  config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD  )


def loop_done(info):
    """ """
    info_tmp = {}
    tmp = []
    for jid, pjids in info.iteritems():
        info_tmp.update( {jid:pjids} )
        c_flag = True
        for pjid in pjids:
            for k, v in info.iteritems():
                if int(k)==pjid:
                    c_flag = False
                    break

            if not c_flag:
                break    ##打破 for pjid in pjids

        if not c_flag:
            continue

        #父任务都完成了
        tmp.append(jid)
        del info_tmp[jid]

    members = get_members(mdb, 0)

    if len(tmp)>0:
        if 0 == len(info_tmp):
            warning_msg = "调度任务循环检查：正常"
            send_sms(members, warning_msg)
            print 'loop finished!'
            return True
        loop_done(info_tmp)
    else:   #死循环了
        out = {}
        for jid, pjids in info.iteritems():
            tmp = []
            for pjid in pjids:
                if pjid in info:
                    tmp.append(pjid)

            out.update( {jid:tmp} )

        log_file = '/data/other/scs_log/checkBind.log'
        warning_msg = "调度任务循环检查：发现异常，不能运行的任务记录在 %s" % log_file
        send_sms(members, warning_msg)
        f = open( log_file, 'a')
        print >> f, out
        f.close()

        print 'warning: is a cycle'

def get_members(mdb,is_worktime,sort_item = None):
    sql = 'SELECT * FROM loop_member where isworktime >= %s' %is_worktime
    data = mdb.query(sql)
    if sort_item == None:
        sort_item = 'priority'
    temp = sorted(data, key = lambda x:x[sort_item], reverse=False)
    result = [item['user'] for item in temp]
    return result

# 把依赖相关的信息组装成一个字典
jconfig = mdb.query("""SELECT * FROM `jconfig` WHERE `open` = 1""")

bind_info = mdb.query("SELECT cid, pid FROM `job_bind`")

info = {}
for row in jconfig:
    pjids = []
    for line in bind_info:
        if row['jid'] == line['cid']:
            pjids.append( int(line['pid']) )

    info.update( {int(row['jid']):pjids} )

loop_done(info)


print 'Done!'
##END