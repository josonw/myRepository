#-*- coding: UTF-8 -*-
"""
创建于2015-09-28
@作者:陈军1654
@简介:更新任务的MapReduce信息
"""
import os

import time
import datetime
import re

from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/hadoop_libs/yarn/' % os.getenv('DC_SERVER_PATH'))

from libs.DB_Mysql import Connection as m_db

from yarn_api_client import ResourceManager

import config


"""
根据SQL保存任务和MapReduce对应关系
"""
def save_jobs_by_sql(query_jobs_sql=None):
    mdb = None

    try:
        mdb = m_db(config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )
        
        applications = mdb.query(query_jobs_sql)

        cur_conf_id = -1

        if not applications or len(applications) == 0:
            return True

        for application in applications:
            """先删除该任务的application信息"""
            mdb.execute("delete from task_app where jid = %d and engine_type='TEZ'"%(application['conf_id']))
            mdb.execute("delete from task_app_dag where jid = %d"%(application['conf_id']))

            diagnostics = ResourceManager(config.HADOOP_YARN_RES_IP).cluster_application(application['application_id'])\
                .data['app']['diagnostics']

            succ_tasks = None

            succ_tasks_p = re.compile(r'dagTasks=\d+((&\d+)*)')
            succ_tasks_m = succ_tasks_p.search(diagnostics)
            if succ_tasks_m:
                succ_tasks = succ_tasks_m.group().replace("dagTasks=","")
                conf_tasks = 0
                for dag_size in succ_tasks.split("&"):
                    save_job_sql = """INSERT INTO task_app_dag (jid,sub_tasks,memory_unit) values(%d,%d,%d)
                    """ % (application['conf_id'],int(dag_size),2)
                    mdb.execute(save_job_sql)
                    conf_tasks = conf_tasks + int(dag_size)

                save_app_sql = """INSERT INTO task_app (jid,sub_tasks,engine_type) values(%d,%d,'TEZ')
                    """ % (application['conf_id'],conf_tasks)
                mdb.execute(save_app_sql)

        mdb.execute("delete from task_res where engine_type='TEZ'")
        mdb.execute("""insert into task_res(jid,containers,vcores,memorys,engine_type) 
            select jid,sum(sub_tasks),sum(sub_tasks),sum(sub_tasks*2),'TEZ' from task_app where engine_type = 'TEZ' group by jid""")
        return True
    except Exception, e:
        print "Failed to save tez applications info.",e
    finally:
        if mdb:
            mdb.close


"""
保存指定时间段执行的任务和MapReduce对应关系
"""
def save_tasks_jobs(start_time=None,end_time=None):
    if not start_time:
        today_str = datetime.date.today().strftime("%Y-%m-%d")
        start_date_str = today_str + " 03:30:00"
        start_time = time.mktime(time.strptime(start_date_str, "%Y-%m-%d %H:%M:%S"))
    if not end_time:
        today_str = datetime.date.today().strftime("%Y-%m-%d")
        end_date_str = today_str + " 08:00:00"
        end_time = time.mktime(time.strptime(end_date_str, "%Y-%m-%d %H:%M:%S"))

    query_jobs_sql = """
            SELECT je.jid conf_id,ehj.application_id
            FROM (select jid,max(eid) eid,max(start_time) start_time from job_entity 
            where status = 6 and start_time > %d and start_time < %d group by jid) je join 
            (select eid,application_id,start_time from entity_app where start_time > %d and start_time < %d and engine_type = 'TEZ') ehj
            on je.eid = ehj.eid and je.start_time = ehj.start_time
        """%(start_time,end_time,start_time,end_time)

    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),\
            "Query tez tasks application: ",query_jobs_sql

    return save_jobs_by_sql(query_jobs_sql)


"""
保存单个Task的Job信息
"""
def save_by_conf_id(conf_id):
    query_jobs_by_conf_id = """
            SELECT je.jid conf_id,ehj.application_id job_id
            FROM (select * from job_entity where status = 6 and jid = %d order by start_time desc limit 1) 
            je join (select * from entity_app where engine_type = 'TEZ') ehj
            on (je.eid = ehj.eid and je.start_time = ehj.start_time)"""%(conf_id)
    
    print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),\
            "Query tez application by conf_id: ",query_jobs_by_conf_id


    return save_jobs_by_sql(query_jobs_by_conf_id)


if __name__ == '__main__':
    mdb = None
    mdb = m_db(config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )
    jids = mdb.query("select distinct jid from task_app")

    for jid in jids:
        save_by_conf_id(jid['jid'])

    mdb.close