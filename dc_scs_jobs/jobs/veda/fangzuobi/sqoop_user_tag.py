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
import config

mysql_db = Connection(host      = config.MYSQL_DFQP_HOST,
                      database  = config.MYSQL_DFQP_DBNAME,
                      user      = config.MYSQL_DFQP_USER,
                      password  = config.MYSQL_DFQP_PSWD)

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
        #insert into user_tag_all(mid,tag,dt) select mid,tag,dt from user_tag where dt='2018-03-16' ON DUPLICATE KEY UPDATE tag=VALUES(tag),dt=VALUES(dt);

        #同步之前先删除当天的数据
        sql = "delete from user_tag where dt=date'%s'" % date
        print sql
        mysql_db.execute(sql)



        args = {
            "ip": 'rm-uf6221u1a0d9963kg.mysql.rds.aliyuncs.com:3306',
            "user":'dc_user',
            "password":'ZGF0YWNlbnRlcg==',
            "db_name": 'datacenter',
            "staging_table":'user_tag_staging',
            "dir": "/dw/veda/dfqp_fzb_relieve_user_tag_changed/dt=%s" % date,
            "table":"user_tag"
            }


        #需要注意的是使用了staging-table(媒介)作为临时表，需要手动建立临时表
        sqoop_cmd = """sqoop export -D mapreduce.job.queuename=sqoop \
                        --connect jdbc:mysql://%(ip)s/%(db_name)s \
                        --username %(user)s \
                        --password %(password)s \
                        --table %(table)s \
                        --staging-table %(staging_table)s \
                        --clear-staging-table \
                        -m 1 \
                        --export-dir %(dir)s \
                        --null-string '\\\\N' \
                        --null-non-string '\\\\N' \
                        --fields-terminated-by '\001' \
                        """ % args


        print sqoop_cmd

        stdin, stdout, stderr = self.ssh.exec_command(sqoop_cmd)
        print stdout.read()
        err_msg =  stderr.read()
        if 'ERROR' in err_msg:
            raise Exception(err_msg)
        sql = "insert into user_tag_all(mid,tag,dt) select mid,tag,dt from user_tag where dt='%s' ON DUPLICATE KEY UPDATE tag=VALUES(tag),dt=VALUES(dt);"%date
        print sql
        mysql_db.execute(sql)
def yesterday():
    import datetime
    return (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

if __name__ == '__main__':
    if len(sys.argv) == 3:
        date = sys.argv[1]
    elif len(sys.argv) == 2:
        date = sys.argv[1]
    else:
        date = yesterday()

    s = ShellExec()
    s.execute(date)
