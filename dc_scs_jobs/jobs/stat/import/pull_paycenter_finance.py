#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat
import paramiko
import config

class ShellExec(object):
    host = config.HIVE_NODE_SSH_IP
    port = config.HIVE_NODE_SSH_PORT
    user = config.HIVE_NODE_SSH_USER
    pkey = config.HIVE_NODE_SSH_PKEY
    def __init__(self):
        self._connect()

    def _connect(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.load_system_host_keys()
        self.ssh.connect(self.host, self.port, self.user, self.pkey)

    def execute(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        return stdout.read(), stderr.read()


class pull_paycenter_finance(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        hql = """
        create external table if not Exists `stage.paycenter_finance`(
                `companyid` int,
                `sid` int,
                `appid` int,
                `pmode` int,
                `income` string,
                `order` int,
                `ptime` string,
                `pamount_unit` string,
                `income_usd` string,
                `pamount_rate` float)
        PARTITIONED BY (
          `dt` string)
        LOCATION
            '/dw/stage/paycenter_finance';
        """

        res = self.hq.exe_sql(hql)
        if res != 0: return res


    def stat(self):
        """ 重要部分，统计内容 """
        self.hq.debug = 0

        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)

        hql = """
        alter table stage.paycenter_finance add if not exists partition(dt='%(ld_end)s') location '/dw/stage/paycenter_finance/%(ld_end)s';
        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today(), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
import_job = pull_paycenter_finance(statDate)
s = ShellExec()


args = {
    "ip": config.MYSQL_DB_SHALIYUN,
    "user": config.MYSQL_DB_SHUSER,
    "password": config.MYSQL_DB_SHPSWD,
    "db_name": config.MYSQL_DB_NAME,
    "dir": "/dw/stage/paycenter_finance/%s" % import_job.hql_dict['ld_end'],
    "table":"paycenter_finance"
}

sqoop_cmd = ("sqoop import "
                     "-D mapreduce.job.queuename=sqoop "
                     "--connect jdbc:mysql://%(ip)s/%(db_name)s "
                     "--username %(user)s "
                     "-m 1 "
                     "--password %(password)s "
                     "--table %(table)s "
                     "--target-dir %(dir)s "
                     "--delete-target-dir "
                     "--null-string '\\\\N' "
                     "--null-non-string '\\\\N' "
                     "--fields-terminated-by '\001' ") % args

#生成统计实例
print sqoop_cmd
out, err = s.execute(sqoop_cmd)
print out

if 'Caused by' in err or 'ERROR ' in err:
    raise Exception(err)
else:
    print err
import_job()
