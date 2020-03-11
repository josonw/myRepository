#-*- coding: UTF-8 -*-
"""
创建于2015-10-08
@作者:陈军1654
@简介：实时任务管理类，如对Spark任务的管理
"""
import time

from sys import path
path.append('%s/hadoop_libs/yarn/' % os.getenv('DC_SERVER_PATH'))

from yarn_api_client import ResourceManager

import config

from libs.DB_Mysql import Connection as m_db

from libs.warning_way import send_sms

class RTWorker(object):


    def __init__(self, mdb):
        self.mdb = mdb  #MySql数据库连接,暂时未使用
        self.tasks = ['user_location.py','RealGamecoin','RealDau','RealChannel/Real-Channel','RealGameparty']  #实时任务的名称集合


    """
    检测实时任务的运行状态，发现异常时报警
    """
    def check(self):
        try:
            print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),"Begin check..."

            """获取正在运行的所有实时任务"""
            self.rm = ResourceManager(config.HADOOP_YARN_RES_IP)  #获取集群资源利用情况
            running_apps = self.rm.cluster_applications(state='RUNNING',
                                         user='hadoop', queue='root.spark').data['apps']['app']

            print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),"Get running apps."

            """查看给定的任务名称是否在查询到的任务中，不在需加入报警任务列表"""
            not_running_tasks = []
            for task_name_all in self.tasks:
                not_running = True
                task_names = task_name_all.split("/")
                for task_name in task_names:
                    if task_name:
                        for app in running_apps:
                            if task_name == app['name']:
                                not_running = False
                                break
                    if not not_running:
                        break

                if not_running:
                    not_running_tasks.append(task_name_all)
                                     
            print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),"Not running real time tasks:",not_running_tasks

            """发送报警"""
            if len(not_running_tasks) > 0 :
                users = ['BlackZhou','JoakunChen','LukeHu']

                """组装报警信息"""
                msg = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) + "集群监控：实时任务"
                for not_run_task in not_running_tasks:
                    msg = msg + not_run_task + "、"
                msg = msg[:-1] + "没有运行"

                send_sms(users, msg)

            print time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),"Check finished."
        except Exception, e:
            except_msg = "Real time task monitor error," + str(e)
            print except_msg
            users = ['JoakunChen']
            send_sms(users, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())) + "集群监控：实时任务检查系统异常" + except_msg)