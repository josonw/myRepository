#! /usr/local/python272/bin/python
# coding: utf-8

from sys import path

import datetime
import paramiko
import time
import psycopg2
import os
import sys

path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from libs.DB_Mysql import Connection

from PublicFunc import PublicFunc
from BaseStat import  pgcluster


class ShellExec(object):

    host = '10.30.101.94'
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

    def execute(self, date):
        sql = "delete from analysis.audit_result_report where fdate=date'%s'" % date
        print sql
        pgcluster.execute(sql)

        #需要指定字段同步，故改为sqoop同步，需要注意的是使用了staging-table作为临时表，需要手动建立临时表
        cmd1 = """sqoop export -D mapreduce.job.queuename=sqoop \
            --connect jdbc:postgresql://10.30.101.240:5432/boyaadw \
            --username analysis \
            -m 1 \
            --password analysis \
            --staging-table audit_result_report_staging \
            --clear-staging-table \
            --export-dir /dw/analysis/audit_result_report/dt=%s \
            --table audit_result_report \
            --columns 'fdate,fgamefsk,fplatformfsk,ftype,fsolve_at,flog_num,flog_error_num,fdetail,fcomment,fstatus,f1validation,f1validation_at,f2validation,f2validation_at,fsystem_fault' \
            --input-null-string '\\\\N' \
            --input-null-non-string '\\\\N' \
            --fields-terminated-by '\001' \
            -- --schema analysis \
            """% date


        print cmd1

        stdin, stdout, stderr = self.ssh.exec_command(cmd1)
        print stderr.read()
        if 'ERROR' in stderr.read():
            raise Exception(stderr.read())



def yestoday():
    import datetime
    return (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

if __name__ == '__main__':
    if len(sys.argv) == 3:
        date = sys.argv[1]
    elif len(sys.argv) == 2:
        date = sys.argv[1]
    else:
        date = yestoday()

    s = ShellExec()
    s.execute(date)
