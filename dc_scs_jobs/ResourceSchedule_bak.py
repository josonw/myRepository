#-*- coding: UTF-8 -*-
"""
创建于2015-09-28
@作者:陈军1654
@简介：集群资源信息类，提供获取当前集群可用资源量、实体任务资源量大小、实体任务是否为hadoop任务等接口
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

            policy = self.mdb.getOne("select config_value from loop_config where config_name='schedule_policy' ")
            self.schedule_policy = policy.get('config_value','DFR')

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

    """
    获取任务检查时间间隔
    返回调度系统循环启动任务检查时间间隔，单位为秒
    """
    def get_check_interval(self):
        return self.short_control_interval*60


    def get_task_size(self,conf_id):
        if not self.task_job_map.has_key(conf_id):
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                    "Can not get the size of ",conf_id
            return None

        return self.task_job_map.get(conf_id)


    def get_pending_res(self):
        pending_vcore_size = 0

        try:
            policy = self.mdb.getOne("select config_value from loop_config where config_name='schedule_policy' ")
            self.schedule_policy = policy.get('config_value','DFR')

            cur_tasks = self.mdb.query("select * from job_running where status = 3")

            for cur_task in cur_tasks:
                if not self.task_job_map.has_key(cur_task['jid']):
                    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                        cur_task['jid']," is not in task_res."
                    continue

                task_progress = self.get_task_progress(cur_task['eid'])['vcores']
                task_pending = (1-task_progress)*(self.task_job_map[cur_task['jid']]['vcores'])
                pending_vcore_size = pending_vcore_size + task_pending

                print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                    "Task",cur_task['jid'],"Entity",cur_task['eid'],\
                    "progress is",task_progress,\
                    "and has",task_pending,"vcores need to be allocated."

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

        try:

            all_tasks = self.mdb.query("select * from job_running where status =2 and jid in (select jid from task_res)")

            return all_tasks
        except Exception, e:
            print "Failed to get remained tasks:",e
            return None
        finally:
            pass


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
        print "the scs loop current schedule policy is %s"%self.schedule_policy
        """剩余的待分配的资源数目"""
        remind_size = self.get_pending_res()['vcores']

        av_ctn_num = 0
        first_key = -1
        hour = int(time.strftime('%H',time.localtime()))
        """key为实体ID,value为对象，value['jid']为配置ID"""
        for item in task_map:
            for key,value in item.iteritems():
                if first_key == -1:
                    first_key = key

                # 这两类任务是随机抽查了几个任务的资源数,得出的初始值(后期可以优化)
                # curl 'http://192.168.0.94:19888/ws/v1/history/mapreduce/jobs/job_1484294914014_928471/
                # tasks' | grep -o startTime |wc -l
                if value.get('jobtype','') in ('pushjobs',):
                    jid_size = 121
                elif value.get('jobtype','')  in (u'明细提取',u'统计分析',u'sql自助查询'):
                    jid_size = 20
                else:
                    jid_size = self.get_task_size(value['jid'])['vcores']
                # 3000以上的大任务,修正到3000,利于资源的利用率
                if hour>8 and jid_size>2000:
                    jid_size=2000

                print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                    "Intend to start task",value['jid'],"entity",key," ,size is ",jid_size

                if value['jid'] in self.vcore_pids:
                    # 核心任务不参与资源判断
                    av_ctn_num += jid_size
                    av_ctn_list.append(key)
                    print "jid: %s is vcore job"%value['jid']
                    continue

                #"""还剩3个任务时，不判断资源，直接启动；"""
                elif (self.balanced_wait_ctn_num < remind_size + av_ctn_num + jid_size) and (len(self.get_remaining_tasks()) > 3):
                    print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                    "No enough containers for start task",value['jid'],"entity",key,",size is ",jid_size
                    print self.balanced_wait_ctn_num, remind_size , av_ctn_num
                    if self.schedule_policy=='FIFO':
                        break
                    else:
                        continue

                av_ctn_num += jid_size
                av_ctn_list.append(key)

        """待分配的资源数目等于0时，义无反顾的启动排第一的任务，
        防止某些超大任务无法启动"""
        if remind_size == 0 and len(av_ctn_list) == 0:
            av_ctn_list.append(first_key)

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
    print rs.update_hadoop_info(1502294400,1502370000)
    #print rs.get_task_progress(1039042)
    #print rs.get_pending_res()
