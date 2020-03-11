#-*- coding: UTF-8 -*-
import os
import time
import sys
import re
import config

from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from BaseHiveSql import BaseHiveSql
from BaseSparkSql import BaseSparkSql

from libs.DB_Mysql import Connection as m_db
import config

class HiveMRSql(BaseHiveSql):
    """
    初始化hive的配置参数
    """
    def init_properties(self):
        job_name_setting = """
                set mapreduce.job.name=%s;
        """%(self.task_name)
        self.exe_sql(job_name_setting)

        mdb = None

        try:
            mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )
            app_count_sql = """SELECT count(*) app_count FROM `task_app` where jid in
                (select jid from job_entity WHERE eid = %s)"""%(self.eid)

            app_count_json = mdb.getOne(app_count_sql)
            app_count = int(app_count_json['app_count'])

            if app_count > 10:
                self.exe_sql("set mapreduce.job.priority=VERY_HIGH;")
                print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                    "Set task(name=",self.task_name,",eid=",self.eid,") job priority to VERY_HIGH"
        except Exception, e:
            print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),\
                "Failed to set task job priority,",e
        finally:
            if mdb:
                mdb.close


    def _deal_hive_log(self,log):
        if "Starting Job = " in log:
            self._deal_mr_log(log)


    def _deal_mr_log(self,log):
        mdb = None

        try:
            mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )

            p = re.finditer(r'Starting Job = job_\d+_\d+,',log)
            for match in p:
                hadoop_job_id = match.group().replace("Starting Job = ","").replace(",","")
                if hadoop_job_id and not self.start_time:
                    query_start_time = """SELECT start_time FROM `job_entity` WHERE EID = %s"""%(self.eid)
                    self.start_time = mdb.getOne(query_start_time)

                if hadoop_job_id and self.start_time:
                    relation = """INSERT INTO `entity_app`(eid,start_time,application_id,engine_type) VALUES (%s,%s,'%s','MR') ON DUPLICATE KEY UPDATE application_id= '%s' """ % (self.eid,\
                        self.start_time['start_time'],hadoop_job_id.replace("job","application"),hadoop_job_id.replace("job","application"))
                    mdb.execute(relation)
        except Exception, e:
            print "Warnning:Can not save mapreduce application ids.If this is a personal test case,you can ignore this problem:",e
        finally:
            if mdb:
                mdb.close

class HiveSpkSql(BaseSparkSql):
    """docstring for ClassName"""
    pass


class Row(dict):
    """访问对象那样访问dict,行结果"""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
