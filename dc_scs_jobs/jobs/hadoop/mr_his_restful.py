#-*- coding: UTF-8 -*-
"""
创建于2015-09-28
@作者:陈军1654
@简介：通过hadoop历史job服务器的restful接口获取hadoop job相关信息，例如map reduce数目、job信息等
"""
import os

import json
import urllib2

#引入mysql数据库操作类及配置文件的路径
from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
path.append('%s/' % os.getenv('SCS_PATH'))

#path.append('E:\\svn\\bydc\\dc_server\\dc_scs_jobs\\')

from libs.DB_Mysql import Connection as m_db

from dc_scs_jobs import config

#job服务器restful接口url地址
global hadoop_restful_url_prefix
hadoop_restful_url_prefix = 'http://'+config.HADOOP_MR_HIS_IP+':19888/ws/v1'

"""
通过Hadoop Job ID获取Map Reduce数目

参数：
    Hadoop Job ID

返回：
    包含map、reduce数目的json
"""
def get_mr_count_by_hjId(hjob_id):
    mr_count = {}
    mr_count["mapCount"] = 0
    mr_count["reduceCount"] = 0

    try:
        tasks_json = json.load(urllib2.urlopen(hadoop_restful_url_prefix + '/history/mapreduce/jobs/' + hjob_id + '/tasks'))

        if ((not tasks_json['tasks']) or (not tasks_json['tasks']['task'])):
            return mr_count

        for task in tasks_json['tasks']['task']:
            attemptsJO = json.load(urllib2.urlopen(hadoop_restful_url_prefix + '/history/mapreduce/jobs/' + hjob_id + '/tasks/' + task['id'] + '/attempts'))
            
            if attemptsJO['taskAttempts']:
                if attemptsJO['taskAttempts']['taskAttempt']:
                    for attempt in attemptsJO['taskAttempts']['taskAttempt']:
                        if('MAP' == attempt['type']):
                            mr_count["mapCount"] += 1
                        elif('REDUCE' == attempt['type']):
                            mr_count["reduceCount"] += 1
    except Exception, e:
        """有时候job被kill时，该请求url找不到，报404错误"""
        print "Not find any mapreduce:",e

    return mr_count


"""
通过实体ID获取任务的总map reduce数目
"""
def get_mr_count_by_eid(eid,start_time):
    mr_count = {}
    mr_count["mapCount"] = 0
    mr_count["reduceCount"] = 0

    jobs = get_hjobs_by_eid(eid,start_time)
    for job in jobs:
        mr_job_count = get_mr_count_by_hjId(job)
        mr_count["mapCount"] = mr_count["mapCount"] + mr_job_count["mapCount"]
        mr_count["reduceCount"] = mr_count["reduceCount"] + mr_job_count["reduceCount"]

    return mr_count


"""
通过hadoop job id获取诊断日志
"""
def _get_diag_log_by_hjId(hjob_id):
    diagLogs = {}
    diagLogsCtent = []
    jsonRslt = json.load(urllib2.urlopen(hadoop_restful_url_prefix + '/history/mapreduce/jobs/' + hjob_id + '/tasks'))
    
    if ((not jsonRslt['tasks']) or (not jsonRslt['tasks']['task'])):
        diagLogs['diagLogs'] = diagLogsCtent
        return diagLogs

    for task in jsonRslt['tasks']['task']:
        if('SUCCEEDED' == task['state']):
            continue
        attemptsJO = json.load(urllib2.urlopen(hadoop_restful_url_prefix + '/history/mapreduce/jobs/' + hjob_id + '/tasks/' + task['id'] + '/attempts'))
        
        if attemptsJO['taskAttempts']:
            if attemptsJO['taskAttempts']['taskAttempt']:
                for attempt in attemptsJO['taskAttempts']['taskAttempt']:
                    if('FAILED' != attempt['state'] or '' == attempt['diagnostics']):
                        continue
                    diagLog = {}
                    diagLog['taskId'] = task['id']
                    diagLog['log'] = attempt['diagnostics']
                    diagLogsCtent.append(diagLog)
                    diagLogs['diagLogs'] = diagLogsCtent
                    return diagLogs
    
    diagLogs['diagLogs'] = diagLogsCtent
    return diagLogs


"""
获取多个hadoop job的诊断日志信息
"""
def _get_jobs_diag_log(jobs):
    jobs_diag_logs = {}
    job_diag_logs = []

    for job in jobs:
        print "url job: ",hadoop_restful_url_prefix,job
        jsonRslt = json.load(urllib2.urlopen(hadoop_restful_url_prefix + '/history/mapreduce/jobs/' + job))
        print "Request job url: ",hadoop_restful_url_prefix + '/history/mapreduce/jobs/' + job
        print "Job result: ",jsonRslt
        if(jsonRslt['job'] is None or jsonRslt['job']['id'] is None or 'SUCCEEDED' == jsonRslt['job']['state']):
            continue
        job_log = {}
        job_log['jobId'] = job
        job_log['jobDiagLog'] = jsonRslt['job']['diagnostics']
        print "job daig log:",job_log['jobDiagLog']
        job_log['taskAttemptsLogs'] = _get_diag_log_by_hjId(job)
        job_diag_logs.append(job_log);

    jobs_diag_logs['jobs'] = job_diag_logs
    #jobs_log_str = json.dumps(jobs_diag_logs);
    return jobs_diag_logs


"""
获取实体任务的所有hadoop job

参数：
    eid:实体任务ID
    start_time:实体任务的开始时间

返回：
    所有hadoop job的ID的集合

抛出：
    数据库异常
"""
def get_hjobs_by_eid(eid,start_time):
    hjobs_id = [];
    mdb = None

    try:
        mdb = m_db(config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )
        print "eid,start_time:",eid," ",start_time
        query = """SELECT hadoop_job_id FROM `entity_hadoop_job` WHERE EID = %s and start_time = %s""" %(eid,start_time)
        qryRslt = mdb.query(query)
        print qryRslt

        for hadoop_job in qryRslt:
            hjobs_id.append(hadoop_job['hadoop_job_id'])
    except Exception, e:
        print "Failed to get hadoop jobs id by entity id."
        raise e
    finally:
        if mdb:
            mdb.close

    return hjobs_id;


"""
获取实体任务的诊断日志

参数：
    eid:实体任务ID
    start_time:实体任务的开始时间

返回：
    Json格式的诊断日志信息
"""
def get_diag_logs_by_eid(eid,start_time):
    hjob_ids = get_hjobs_by_eid(eid,start_time)
    
    jobs_diag_logs = {}
    job_diag_logs = []
    jobs_diag_logs['jobs'] = job_diag_logs

    if hjob_ids:
        print "begin get diagnostics logs of ",hjob_ids
        jobs_diag_logs = _get_jobs_diag_log(hjob_ids)

    #jobs_log_str = json.dumps(jobs_diag_logs);
    return jobs_diag_logs


"""
获取Hadoop Job信息

参数：
    hjob_id:Hadoop Job ID

返回：
    Json格式的JOB信息
"""
def get_job_info(hjob_id):
    job = None

    try:
        job = json.load(urllib2.urlopen(hadoop_restful_url_prefix + '/history/mapreduce/jobs/' + hjob_id))
    except Exception, e:
        print "Can not get job information from mapreduce history restful server:",e

    return job


if __name__ == '__main__':
   import sys
   job_id = sys.argv[1].replace("application", "job")
   print job_id
   res = get_mr_count_by_hjId(job_id)
   print res
