#-*- coding: UTF-8 -*-
import os
import time
import sys
import re

from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseHiveSql import BaseHiveSql
from libs.DB_Mysql import Connection as m_db
import config

class HiveOnTezSql (BaseHiveSql):

    """
    初始化hive的配置参数
    """
    def init_properties(self):
        self.application_id = None

        hive_properties = {}

        hive_properties['hive.execution.engine'] = "tez"
        hive_properties['tez.name.prefix'] = self.task_name + "-"
        #hive_properties['mapreduce.map.memory.mb'] = "1024"
        hive_properties['hive.tez.container.size'] = "2048"
        hive_properties['tez.task.resource.memory.mb'] = "2048"
        hive_properties['hive.tez.java.opts'] = "-Xmx1700m"
        #hive_properties['hive.exec.reducers.bytes.per.reducer'] = "10240000"
        #hive_properties['tez.runtime.io.sort.mb'] = "512"

        mdb = None

        try:
            mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )
            query = """SELECT sub_tasks FROM `task_app_dag` tad join
                (select jid from job_entity WHERE eid = %s) je on tad.jid = je.jid order by tad.id asc"""%(self.eid)
            dags_his = mdb.query(query)

            dag_acount = ""
            if dags_his:
                for dag in dags_his:
                    dag_acount = dag_acount + str(dag['sub_tasks']) + ","

            if "" != dag_acount:
                hive_properties['tez.am.history.dags.tasks'] = dag_acount[:-1]
        except Exception, e:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                "Failed to set task number and dag number of application,",e
        finally:
            if mdb:
                mdb.close

        for property_key,property_val in hive_properties.items():
            property_setting = "set " + property_key + "=" +  property_val + ";"
            self.exe_sql(property_setting)

        """记录已开始的DAG数目，不包括File Merge类型的DAG"""
        self.dag_amount = 0

        self.cur_merge_flag = -1

        self.engine = "TEZ"


    def _deal_hive_log(self,log):
        if "Executing on YARN cluster with App id " in log:
            self.cur_merge_flag = -1

            pattern = re.compile(r'File Merge:')
            match = pattern.search(log)

            """DAG为MapReduce时，计入实时DAG数目统计"""
            if not match:
                self.dag_amount = self.dag_amount + 1
                self._deal_tez_log(log)

        if self.cur_merge_flag == -1:
            merge_pattern = re.compile(r'File Merge: (\d+)\(\+(\d+)\)/(\d+)')
            merge_match = merge_pattern.search(log)
            if merge_match:
                merge_size_str = merge_match.group()
                merge_size = int(merge_size_str[merge_size_str.index("/")+1:len(merge_size_str)])
                self.cur_merge_flag = merge_size
                """DAG为File Merge且资源量大于2时，才计入统计，防止有时该日志未打印"""
                if merge_size >= 3:
                    self.dag_amount = self.dag_amount + 1
                    self._deal_tez_log(log)


    def _deal_tez_log(self,log):
        mdb = None

        try:
            print "Entity id:",self.eid
            mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )

            if self.application_id:
                save_dag = """update `entity_app_dag` set dag_amount = %s where application_id = '%s'""" %(self.dag_amount,self.application_id)
                mdb.execute(save_dag)
                return

            pattern = re.compile(r'application_\d+_\d+')
            match = pattern.search(log)
            if match:
                application_id = match.group()

                query_start_time = """SELECT start_time FROM `job_entity` WHERE EID = %s"""%(self.eid)
                self.start_time = mdb.getOne(query_start_time)

                if self.start_time:
                    relation = """INSERT INTO `entity_app`(eid,start_time,application_id,engine_type) VALUES (%s,%s,'%s','TEZ')""" % (self.eid,self.start_time['start_time'],application_id)
                    mdb.execute(relation)
                    self.application_id = application_id

                    save_dag = """INSERT INTO `entity_app_dag`(application_id) VALUES ('%s')""" % (self.application_id)
                    mdb.execute(save_dag)
        except Exception, e:

            print "Warnning:Can not save tez application ids.If this is a personal test case,you can ignore this problem:",e
        finally:
            if mdb:
                mdb.close
