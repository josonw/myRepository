#! /usr/local/python272/bin/python
# coding: utf-8

import os
import sys
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))

import datetime
import paramiko

import config

"""
主要拉取支付配置类数据
tommyjiang 2017-04-26
"""

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


if __name__ == '__main__':
    if len(sys.argv) == 2:
        date = sys.argv[1]
    elif len(sys.argv) == 3:
        date = sys.argv[1]
    else:
        print '参数错误'
        sys.exit(1)

    s = ShellExec()
    args = {
        "ip": config.MYSQL_DB_SHALIYUN,#12.5统一将内网修改成外网地址
        "user": config.MYSQL_DB_SHUSER,
        "password": config.MYSQL_DB_SHPSWD,
        "db_name": config.MYSQL_DB_NAME,
        "dir": "/dw/stage/pay_stream/%s" % date,
    }
    num_date = date.replace("-", "")
    sqls = {"paycenter_payconf": '"select * from paycenter.paycenter_payconf where \$CONDITIONS"',
            "paycenter_chanel": '"select * from paycenter.paycenter_chanel where \$CONDITIONS"',
            "paycenter_rate": '"select * from paycenter.paycenter_rate where \$CONDITIONS"',
            "paycenter_apps": '"select * from paycenter.paycenter_apps where \$CONDITIONS"',
            "paycenter_apps_attr":'"select * from paycenter.paycenter_apps_attr where \$CONDITIONS"',
            "paycenter_site":'"select * from paycenter.paycenter_site where \$CONDITIONS"',
            "paycenter_game":'"select * from paycenter.paycenter_game where \$CONDITIONS"',
            "paycenter_lang":'"select * from paycenter.paycenter_lang where \$CONDITIONS"'
            }

    for tb, sql in sqls.iteritems():
        args["dir"] = "/dw/stage/%s/%s" % (tb, date)
        args["sql"] = sql

        sqoop_cmd = ("sqoop import "
                     "-D mapreduce.job.queuename=sqoop "
                     "--connect jdbc:mysql://%(ip)s/%(db_name)s "
                     "--username %(user)s "
                     "-m 1 "
                     "--password %(password)s "
                     "--query %(sql)s "
                     "--target-dir %(dir)s "
                     "--delete-target-dir "
                     "--null-string '\\\\N' "
                     "--null-non-string '\\\\N' "
                     "--fields-terminated-by '\001' ") % args
        print sqoop_cmd
        out, err = s.execute(sqoop_cmd)
        print out

        if 'Caused by' in err or 'ERROR ' in err:
            raise Exception(err)
        else:
            print err
