#-*- coding: UTF-8 -*-
import time
import sys
import os
from itertools import chain
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
import config
from libs.DB_Mysql import Connection as m_db
from libs.warning_way import send_sms
import datetime


"""检查核心同步任务是否在指定时间内完成或者正在运行"""



def getkids_RecurTree(pids,bind_tree,dept=1):
    # 查找某些cids的所有fujids
    temp=[]
    allkids = []
    for jid in pids:
        for item_dept in bind_tree:
            if jid == item_dept[1]:
                temp.append([item_dept[0],item_dept[1],dept])

    if len(temp)==0:
        return []
    elif dept > 100:    #防止出现任务依赖循环，某任务大于100层后是自循环
        return [[0,0,0]]
    else:
        allkids.extend(temp)
        temp_pids=[x[0] for x in temp]

        dept = dept + 1
        allkids.extend(getkids_RecurTree(temp_pids,bind_tree,dept))
    return allkids

def check_vcore_jobs(str_day, mdb, jidstr):
    """检查核心同步任务是否在指定时间内完成或者正在运行 """
    check_result = []
    sql = """
    SELECT a.jid, b.calling, a.status FROM job_entity a
    join jconfig b on a.jid=b.jid
    where a.d_time = '%s' and a.jid in (%s)
    """ %(str_day,jidstr)
    data = mdb.query(sql)

    for item in data:
        if item['status'] not in (3,6):
            check_result.append({'jid':item['jid'], 'msg':"%s:%s"%(item['jid'], item['calling'] ) } )

    return check_result


def add_pjid(mdb,check_result):
    # 同时找出这些任务的父任务
    ids = [k['jid'] for k in check_result]

    bind_info = mdb.query("SELECT cid, pid FROM `job_bind`")
    bind_tree = [[x['pid'],x['cid']] for x in bind_info]

    allkids_list = getkids_RecurTree(ids, bind_tree, 1)
    pjid_str=''
    if allkids_list:
        allkids_list = [[item[0],item[1]] for item in allkids_list]   #去掉深度层级
        allkids = list(chain.from_iterable(allkids_list))      #剥掉内层列表符号
        allkids = list(set(allkids ))         #去重

    if allkids:
        pjid_str=','.join([str(kids) for kids in allkids])

    return pjid_str


def do_stop(mdb,str_day,jidstr,jobstr):
    """与这些核心任务不相关的任务都先暂停"""
    mdb.execute("update job_running set status=7 where jid not in (%s) """%jidstr)
    time.sleep(600)
    # 十分钟后再检查这些核心任务的父任务有没有在跑
    # 没有再跑就电话告警
    data = check_vcore_jobs(str_day, mdb, jobstr)
    return data


def get_members(mdb):
    sql = 'SELECT * FROM loop_member where priority<=3'
    data = mdb.query(sql)
    data = sorted(data, key = lambda x:x['priority'], reverse=False)
    result = [item['user'] for item in data]
    return result

def main(str_day, mdb, jobstr):
    # 做两次检查，间隔10分钟，将暂停一部分任务后看核心任务的父任务能否优先启动
    # 十分钟后还没有启动就电话告警

    h = time.strftime("%H", time.localtime())
    result = check_vcore_jobs(str_day, mdb, jobstr)
    if result:
        jidstr = add_pjid(result)
        data = do_stop(mdb,str_day,jidstr,jobstr)
        if data:
            warning_msg = "核心任务监控:\n%s\n没有在%s点前完成"%(',\n'.join([item['msg'] for item in data]),h)
            send_sms(get_members(mdb), warning_msg, 8)
            print str_day
            print warning_msg
        else:
            # 父任务已经完成或者正在运行再开启所有关闭的任务
            mdb.execute("update job_running set status=2 where status=7 and d_time ='%s' "%str_day)

    else:
        warning_msg= "核心任务已完成"
        send_sms(get_members(mdb), warning_msg, 3)



if __name__ == '__main__':
    # 对应单机数据的任务id [1204,1251,1263,1335,1199,1707,1902,1705,1906,1835]
    job_list = [2198,2202,2244,2256,2317,2387,2388,2440,2466,2469]
    jobstr = ','.join([str(i) for i in job_list])

    mdb = m_db(config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD  )
    str_day = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    main(str_day, mdb, jobstr)
    print 'Done!'
    ##END
