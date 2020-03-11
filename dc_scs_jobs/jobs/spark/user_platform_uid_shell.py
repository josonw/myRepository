#! /usr/local/python272/bin/python
# coding: utf-8

from sys import path
import os
import datetime
import paramiko
import sys


class ShellExec(object):

    host = '192.168.0.94'
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

    def execute(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        return stdout.read(), stderr.read()


def send_file():
    os.system("cd $SCS_PATH/jobs; scp -P 3600 spark/user_platform_uid_spark.py hadoop@192.168.0.94:/data/workspace/")



if __name__ == '__main__':

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]


    send_file()

    s = ShellExec()
    out, err = s.execute(
        "cd /data/workspace; "
        "source ~/.bash_profile; "
        "spark-submit --master yarn-client --py-files lib.zip  user_platform_uid_spark.py %s" % stat_date )
    print out, err





