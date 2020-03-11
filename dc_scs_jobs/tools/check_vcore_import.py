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
import datetime
reload(sys)
sys.setdefaultencoding( "utf-8" )

"""
检查三个关键同步任务是否在指定时间完成
-----|-------------------------------------------------|
jid  |  calling                                        |
-----|-------------------------------------------------|
800  | stat/sqoop_import.py bpid_platform_game_ver_map |
1712 | sqoop/pg_to_hive.py bpid_map                    |
2134 | sqoop/pg_to_hive.py bpid_map_bud                |
-----|-------------------------------------------------|
"""

def check_vcore_jobs(str_day, mdb, jidstr):
    """ """
    msg = []
    sql = """
    SELECT a.jid, b.calling, a.status FROM job_entity a
    join jconfig b on a.jid=b.jid
    where a.d_time = '%s' and a.jid in (%s)
    """ %(str_day,jidstr)
    data = mdb.query(sql)
    # 默认告警优先级
    priority = 3

    for item in data:
        if item['status'] <> 6:
            priority = 8
            msg.append("%s: %s: %s"%(item['jid'],item['calling'],u'失败'))
        else:
            msg.append("%s: %s: %s"%(item['jid'],item['calling'],u'完成'))

    check_result = "%s%s"%(u"pg导入hive任务监控:\n",'\n'.join(msg))

    return check_result,priority

def get_members(mdb,is_worktime,sort_item = None):
    sql = 'SELECT * FROM loop_member where isworktime >= %s' %is_worktime
    data = mdb.query(sql)
    if sort_item == None:
        sort_item = 'priority'
    temp = sorted(data, key = lambda x:x[sort_item], reverse=False)
    result = [item['user'] for item in temp]
    return result

def do_alarm(str_day, mdb, jidstr):
    members = get_members(mdb,0)

    print "pg导入hive任务监控:"
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
    print members

    result = check_vcore_jobs(str_day, mdb, jidstr)

    send_sms(members, result[0], result[1])


if __name__ == '__main__':

    job_list = [800,1712,2134]
    jobstr = ','.join([str(i) for i in job_list])

    mdb = m_db(config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD  )
    str_day = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    do_alarm(str_day, mdb, jobstr)
