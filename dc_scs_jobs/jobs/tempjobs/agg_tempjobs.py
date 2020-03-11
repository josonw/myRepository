#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
import paramiko

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
# from BaseStatModel import BaseStatModel
from BaseStat import BaseStat
import service.sql_const as sql_const
from libs.DB_Mysql import Connection as m_db
import config
from libs.ImpalaSql import ImpalaSql

reload(sys)
sys.setdefaultencoding( "utf-8" )


class ShellExec(object):

    host = '10.30.101.216'
    port = 3600
    user = 'hadoop'
    pkey = '/home/hadoop/.ssh/id_rsa'

    def __init__(self):
        self._connect()

    def _connect(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.load_system_host_keys()
        self.ssh.connect(self.host, self.port, self.user, self.pkey)

    def execute(self, cmd, timeout):
        stdin, stdout, stderr = self.ssh.exec_command(cmd, timeout)
        return stdout.read(), stderr.read()


class agg_tempjobs(BaseStat):

    def create_tab(self):
        """ 重要部分，统计内容 """
        mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )
        data = mdb.getOne(""" select * from tjob_running where jid = %s """ %self.eid)
        if data.get('jobtype','') in (u'开放查询'):
            # 要转成utf-8格式的编码
            hql = data.get("job_create_tab_sql")
            if hql:
                hql = hql.encode('utf-8')
                impala = ImpalaSql(host=config.IMPALA_DB_IP, port=config.IMPALA_DB_PORT)
                res = impala.exe_sql(hql)
                if res != 0:return res

    def stat(self):
        """ 重要部分，统计内容 """
        mdb = m_db( config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD )
        data = mdb.getOne(""" select * from tjob_running where jid = %s """ %self.eid)

        # res = self.sql_exe(hql)
        # if res != 0:return res
        if isinstance(data, dict):
            # 把任务设定在tempjobs资源池
            # 控制map，reduce 的最大个数
            hivecmds = ["set mapreduce.job.queuename=tempjobs",
                        "set hive.exec.reducers.max=10"]

            data['mapreduce_cnt'] = ';'.join(hivecmds)


        if data.get('jobtype','') in (u'明细提取',u'统计分析'):
            hql = """
            %(mapreduce_cnt)s;
            insert overwrite local directory '/data/workspace/query_data/%(jid)s'
            row format delimited fields terminated by ','
            null defined as ''
            %(tempsql)s;
            """%data
            # 要转成utf-8格式的编码
            hql = hql.encode('utf-8')
            res = self.hq.exe_sql(hql)
            if res != 0:return res

        elif data.get('jobtype','') in (u'sql自助查询'):
            s = ShellExec()

            data['tempsql'] = data['tempsql'].replace('"',"'")

            execmd = """source ~/.bash_profile; hive -e "set hive.cli.print.header=true;%(mapreduce_cnt)s; %(tempsql)s" > /data/workspace/query_data/%(jid)s/%(jid)s.csv;"""%data
            execmd = execmd.encode('utf-8')

            cmds = [
                    {'cmd':"mkdir -p /data/workspace/query_data/%(jid)s/; "%data,'timeout':10},
                    {'cmd':execmd,'timeout':data['maxtime']-60 },
                    {'cmd':"sed -i 's/\\t/,/g' /data/workspace/query_data/%(jid)s/%(jid)s.csv; "%data,'timeout':20},
                    {'cmd':"cd /data/workspace/query_data/%(jid)s/; tar -zcvf %(jid)s.tar.gz %(jid)s.csv"%data,'timeout':30}
                   ]

            for item in cmds:
                print item
                out, err = s.execute(item['cmd'], item['timeout'])
                print out, err

        elif data.get('jobtype','') in (u'开放查询'):
            hql = data.get('tempsql', "").encode('utf-8')   #impala 执行统计
            impala = ImpalaSql(host=config.IMPALA_DB_IP, port=config.IMPALA_DB_PORT)
            res = impala.exe_sql(hql)
            if res != 0:return res

            hql = """
            insert overwrite local directory '/data/workspace/query_data/%(jid)s'
            row format delimited fields terminated by ','
            null defined as ''
            select * from %(res_tb_name)s;
            """%data
            hql = hql.encode('utf-8')
            res = self.hq.exe_sql(hql)   #使用hive将数据结果导出
            if res != 0:return res

        else:
            raise Exception(u"该任务不存在")




#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话,日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取,实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_tempjobs(statDate)
a()
