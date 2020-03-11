#-*- coding: UTF-8 -*-
import time
import os
import sys
from itertools import chain
from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from libs.DB_Mysql import Connection as m_db
import config

reload(sys)
sys.setdefaultencoding( "utf-8" )


def getkids_RecurTree(pids,bind_tree,dept=1):
    # 查找某些pids的所有子jids
    # cids的所有父jids
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

def getpids_from_bindinfo(ids, bind_tree):
    # 找出所有父任务
    allkids_list = getkids_RecurTree(ids, bind_tree, 1)
    if allkids_list:
        allkids_list = [item[0] for item in allkids_list if len(item)>0 and item[0]>0]   #去掉深度层级
        allkids = set(allkids_list )         #去重
        return allkids
    else:
        return set()

if __name__ == '__main__':
    mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )
    bind_info = mdb.query("SELECT cid, pid FROM `job_bind`")
    bind_tree=[[x['cid'],x['pid']] for x in bind_info]
    allkids_list = getkids_RecurTree([800],bind_tree,1)
    print allkids_list