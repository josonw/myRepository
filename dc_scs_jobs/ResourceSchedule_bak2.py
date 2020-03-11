#-*- coding: UTF-8 -*-
"""
创建于2015-09-28
@作者:陈军1654
@简介：集群资源信息类，提供获取当前集群可用资源量、实体任务资源量大小、实体任务是否为hadoop任务等接口
@TommyJiang修改与2017-08-11

"""
import os
import config
import traceback
import time
import sys
from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
path.append('%s/jobs/hadoop' % os.getenv('SCS_PATH'))
path.append('%s/hadoop_libs/yarn/' % os.getenv('DC_SERVER_PATH'))

import mr_stat_job_detail
import tez_stat_job_detail

from libs.DB_Mysql import Connection as m_db

from yarn_api_client import ResourceManager
from job_bind_info import getkids_RecurTree,getpids_from_bindinfo

reload(sys)
sys.setdefaultencoding( "utf-8" )


class ResourceSchedule(object):


    """
    初始化方法，设置Hadoop Yarn Restful接口url地址、最大运行资源量等
    """
    def __init__(self, res_ip=config.HADOOP_YARN_RES_IP):
        """控制资源利用的短期时间范围，单位为分钟;根据资源利用情况，计算这个时间段内还可以添加的任务；
        通过控制每个短期时间段的资源个数，达到控制整个运行过程的充分利用
        """
        self.mdb = m_db(config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )

        self.short_control_interval = 1

        """推荐的最佳并行任务数"""
        self.optimal_task_num = 20

        """集群恒定待运行容器数目"""
        self.balanced_wait_ctn_num = 3700

        """集群最大可运行容器数目"""
        self.max_container_num = 401

        """总任务最佳运行总时间，单位为分钟"""
        self.optimal_running_time = 150

        self.task_job_map = {}

        """初始化各参数"""
        self._init_paras()

        self.rm = None
        """DFR公平式动态资源策略，默认策略
           FIFO先进先出策略，在紧急告警时使用"""
        self.schedule_policy="DFR"

    """
    初始化资源管理参数,如最大待分配资源数
    """
    def _init_paras(self):
        self.init_conf_size()
        self.init_pids()
        self.init_bigjids()

    def init_conf_size(self):
        self.task_job_map = {}

        try:

            all_conf = self.mdb.query("""select tr.jid,containers,vcores,memorys,tce.engine
                from task_res tr join task_computer_engine tce
                on (tr.jid = tce.jid and tr.engine_type = tce.engine)
                union select trn.jid,containers,vcores,memorys,
                trn.engine_type engine from task_res trn where not exists
                (select 1 from task_computer_engine tcen where tcen.jid = trn.jid)""")
            for conf_job in all_conf:
                task_res = {}
                task_res['containers'] = conf_job['containers']
                task_res['vcores'] = conf_job['vcores']
                task_res['memorys'] = conf_job['memorys']
                task_res['engine'] = conf_job['engine']
                self.task_job_map[conf_job['jid']] = task_res

            all_app = self.mdb.query("""(select ta.jid,ta.sub_tasks,ta.engine_type from task_app ta join task_computer_engine tce
                on (ta.jid = tce.jid and ta.engine_type = tce.engine) order by ta.jid asc,ta.id asc) union all
                (select tan.jid,tan.sub_tasks,tan.engine_type from task_app tan where not exists
                (select 1 from task_computer_engine tcen where tcen.jid = tan.jid) order by tan.jid asc,tan.id asc)
                 """)

            for app in all_app:
                if not self.task_job_map.has_key(app['jid'])\
                 or self.task_job_map[app['jid']]['engine'] != app['engine_type']:
                    continue

                if not self.task_job_map[app['jid']].get('apps'):
                    self.task_job_map[app['jid']]['apps'] = []

                task_app = {}
                task_app['size'] = app['sub_tasks']
                self.task_job_map[app['jid']]['apps'].append(task_app)

            all_dag = self.mdb.query("""select tad.jid,tad.sub_tasks,tad.memory_unit from task_app_dag tad join task_computer_engine tce
                on tad.jid = tce.jid where tce.engine = 'TEZ' order by tad.jid asc,tad.id asc
                """)
            for dag in all_dag:
                if not self.task_job_map.has_key(dag['jid']) \
                    or self.task_job_map[dag['jid']]['engine'] != "TEZ":
                    continue

                """资源数目小于3的，认定为File Merge DAG,不计入统计"""
                if dag['sub_tasks'] < 3:
                    continue

                if not self.task_job_map[dag['jid']]['apps'][0].get('dags'):
                    self.task_job_map[dag['jid']]['apps'][0]['dags'] = []

                app_dag = {}
                app_dag['size'] = dag['sub_tasks']
                app_dag['memory_unit'] = dag['memory_unit']
                self.task_job_map[dag['jid']]['apps'][0]['dags'].append(app_dag)

        except Exception, e:
            print "Failed to init conf task size:",e
            return None
        finally:
            pass

    def init_pids(self):
        self.vcore_pids=set()
        cid = self.mdb.getOne("select config_value from loop_config where config_name='vcore_jobs'")
        cids = [int(i) for i in cid.get("config_value").split(",")]

        bind_info = self.mdb.query("SELECT cid, pid FROM `job_bind`")
        bind_tree = [[x['pid'],x['cid']] for x in bind_info]
        self.vcore_pids = getpids_from_bindinfo(cids, bind_tree)
        print "vcore pid jids is : %s"%self.vcore_pids

    def init_bigjids(self):
        self.bigjids=set()
        threshold = self.mdb.getOne("select config_value from loop_config where config_name='bjobs_containers'")
        jids = self.mdb.query("select jid from task_res where containers>%s group by jid"%threshold.get('config_value',2000))

        self.bigjids=set([item['jid'] for item in jids])
        self.bigjids_str=','.join([str(item['jid']) for item in jids])
        print "big jobs jids is : [%s]"%self.bigjids_str

    """
    获取任务检查时间间隔
    返回调度系统循环启动任务检查时间间隔，单位为秒
    """
    def get_check_interval(self):
        return self.short_control_interval*10


    def get_task_size(self,conf_id):
        if not self.task_job_map.has_key(conf_id):
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                    "Can not get the size of ",conf_id
            return None

        return self.task_job_map.get(conf_id)


    def get_pending_res(self):
        pending_vcore_size = 0

        try:
            cur_tasks = self.mdb.query("select * from job_running where status = 3")

            for cur_task in cur_tasks:
                if not self.task_job_map.has_key(cur_task['jid']):
                    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                        cur_task['jid']," is not in task_res."
                    continue

                task_progress = self.get_task_progress(cur_task['eid'])['vcores']
                task_pending = (1-task_progress)*(self.task_job_map[cur_task['jid']]['vcores'])
                pending_vcore_size = pending_vcore_size + task_pending

                # print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                #     "Task",cur_task['jid'],"Entity",cur_task['eid'],\
                #     "progress is",task_progress,\
                #     "and has",task_pending,"vcores need to be allocated."

            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                    "Total pending:",pending_vcore_size,"vcores need to be allocated."
        except Exception, e:
            print "Failed to computer remain tasks size:",e
            remain_size = {}
            remain_size['vcores'] = self.balanced_wait_ctn_num/2
            return remain_size
        finally:
            pass

        remain_size = {}
        remain_size['vcores'] = int(pending_vcore_size)
        return remain_size


    def get_remaining_tasks(self):
        # 找出当前任务中运行的大任务个数
        sql = """
        select
        count(r.jid) frun_bjids
        from job_running r
        join task_computer_engine e
        on r.jid=e.jid
        join task_res t
        on e.jid=t.jid and e.engine=t.engine_type
        where r.status=3 and r.jid in (%s)"""%(self.bigjids_str)
        ftotaljid = self.mdb.getOne(sql)
        if not ftotaljid:
            ftotaljid = {'frun_bjids':0}

        return ftotaljid

    def getPending_containers(self):
        # 从yarn中找出挂起的containers个数
        metrics_info={}
        try:
            self.rm = ResourceManager(config.HADOOP_YARN_RES_IP)
            metrics_info=self.rm.cluster_metrics().data
        except Exception, e:
            print e

        if not metrics_info:
            metrics_info={'containersPending':100,'availableVirtualCores':10,'appsPending':50}
        else:
            metrics_info=metrics_info.get('clusterMetrics',{'containersPending':100,'availableVirtualCores':10,'appsPending':50})
        return metrics_info

    """
    判断任务是否为包含MapReduce的Hadoop任务

    参数：
        conf_id:配置任务的ID

    返回：
        True 是hadoop任务
        False 不是hadoop任务，
        异常时返回Fasle
    """
    def is_hadoop_task(self,conf_id):
        try:
            if self.task_job_map.has_key(conf_id):
                return True
            return False
        except Exception, e:
            return False


    """
    从待启动的一批任务中，选择与当前资源利用相符的一部分任务启动

    参数：
        task_map:待启动任务Map集合，形如[{eid:1103,data:val}{eid:1104,data:val},],
                 其中val为select * from job_running where eid = 1103的值

    返回：
        eids:可以启动的实体任务ID集合
    """
    def get_need_start_tasks(self,task_map):
        av_ctn_list = []
        """key为实体ID,value为对象，value['jid']为配置ID"""
        flag=1
        threshold= self.mdb.getOne("select config_value from loop_config where config_name='yarn_metrics_threshold'")
        temp = threshold.get('config_value','250|5|10,50|50|30').split(',')

        threshold_v = [[int(j) for j in i.split('|')] for i in temp]

        for item in task_map:
            for key,value in item.iteritems():
                yarn_info = self.getPending_containers()

                if value['jid'] in self.vcore_pids:
                    # 核心任务不参与资源判断
                    av_ctn_list.append(key)
                    print "jid: %s is vcore job"%value['jid']
                    continue

                elif yarn_info['containersPending']<threshold_v[1][0] and yarn_info['availableVirtualCores']>threshold_v[1][1] and yarn_info['appsPending']<threshold_v[1][2]:
                    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                    "yarn have enough vcores for start task",value['jid'],"entity",key, yarn_info
                    av_ctn_list.append(key)
                    continue

                #"""资源不够时,则不启动任务"""
                elif yarn_info['containersPending'] >= threshold_v[0][0] or yarn_info['availableVirtualCores']<=threshold_v[0][1] or yarn_info['appsPending']>=threshold_v[0][2]:
                    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                    "yarn no enough resource for start task",value['jid'],"entity",key, yarn_info
                    flag=0
                    break

                #""" 如果5个以上的大任务在运行不能启动任务 """
                elif self.get_remaining_tasks()['frun_bjids'] > 5:
                    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                    "No enough containers for start task",value['jid'],"entity",key,",size is ",jid_size
                    flag=0
                    break

                av_ctn_list.append(key)

            if flag==0:
                break

        print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),"Resource schedule batch start task: ",av_ctn_list
        return av_ctn_list


    """
    更新Task的MapReduce信息
    一般为统计任务正常运行当天的Task，如当天执行出错，任务有重复执行，可以不更新调用此方法
    默认为凌晨3点半到8点的任务运行情况

    参数：
        start_time:时间戳，统计任务的开始时间
        end_time:时间戳，统计任务的结束时间

    返回：
        True：更新成功
        False：更新失败
    """
    def update_hadoop_info(self,start_time=None,end_time=None):
        try:
            result = mr_stat_job_detail.save_tasks_jobs(start_time,end_time)\
                and tez_stat_job_detail.save_tasks_jobs(start_time,end_time)
            self._init_paras()
            return result
        except Exception, e:
            print "Update total hadoop info failed:",e
            return False


    """
    保存新配置任务的job信息

    参数：
        配置任务的ID，JID

    返回：
        True：更新成功
        False：更新失败
    """
    def save_new_task(self,conf_id):
        try:
            if '0' != self.task_job_map.get(conf_id, '0') and 'TEZ' == self.task_job_map[conf_id].get('engine', ''):
                rst = tez_stat_job_detail.save_by_conf_id(conf_id)
            else:
                rst = mr_stat_job_detail.save_by_conf_id(conf_id)

            return rst
        except Exception, e:
            print "Save new task info failed:",e
            return False


    def get_task_progress(self,eid=0):
        progress = {}
        progress['vcores'] = 0

        try:
            running_apps = self.mdb.query("""select je.jid,je.eid,ea.application_id from
                (select eid,start_time,jid from job_entity where eid = %d order by start_time desc limit 1) je
                join entity_app ea
                on (je.eid = ea.eid and je.start_time = ea.start_time) """%(eid))

            if not running_apps or 0 == len(running_apps):
                return progress

            task_info = self.task_job_map[running_apps[0]['jid']]

            if "TEZ" == task_info['engine']:
                dag_info = self.mdb.getOne("""select dag_amount dag_acount,jeea.application_id from
                    (select je.jid,je.eid,ea.application_id from (select eid,start_time,jid from job_entity where eid = %d order by start_time desc limit 1) je
                    join entity_app ea on je.eid = ea.eid and je.start_time = ea.start_time where ea.engine_type = 'TEZ') jeea
                    join entity_app_dag ead
                    on jeea.application_id = ead.application_id"""%(eid))
                return self.get_tez_task_progress(task_info,dag_info)
            else:
                return self.get_mr_task_progress(task_info,running_apps)
        except Exception, e:
            print "Failed to get task(",eid,") progress:",e
            print traceback.format_exc()
        finally:
            pass

        return progress


    def get_mr_task_progress(self,task_info,running_apps=None):
        progress = {}
        progress['vcores'] = 0

        completed_vcores = 0
        his_apps = task_info['apps']
        for index in range(0,len(running_apps)):
            if len(his_apps) > index:
                self.rm = ResourceManager(config.HADOOP_YARN_RES_IP)
                app_progress = float(self.rm.cluster_application(running_apps[index]['application_id']).data['app']['progress']/100)
                completed_vcores = completed_vcores + his_apps[index]['size']*app_progress

        progress['vcores'] = float(completed_vcores/task_info['vcores'])
        if progress['vcores'] > 1:
            progress['vcores'] = 1
        return progress


    def get_tez_task_progress(self,task_info,dag_info=None):
        progress = {}
        progress['vcores'] = 0
        completed_vcores = 0

        dag_num = dag_info['dag_acount']
        app = task_info['apps'][0]
        if dag_num == 0 or not app.get('dags'):
            return progress

        self.rm = ResourceManager(config.HADOOP_YARN_RES_IP)
        app_progress = float(self.rm.cluster_application(dag_info['application_id']).\
            data['app']['progress']/100)

        if len(app['dags']) >= dag_num:
            completed_vcores = completed_vcores + app['dags'][dag_num-1]['size']*app_progress

        for index in range(0,dag_num-1):
            if len(app['dags']) > index:
                completed_vcores = completed_vcores + app['dags'][index]['size']

        progress['vcores'] = float(completed_vcores/task_info['vcores'])
        if progress['vcores'] > 1:
            progress['vcores'] = 1
        return progress



if __name__ == '__main__':
    rs = ResourceSchedule()
    #print rs.update_hadoop_info(1450713600,1450785600)
    #print rs.get_task_progress(1103)
    print rs.get_remaining_tasks()
    print rs.getPending_containers()
