# -*- coding: utf:8 -*-
# Author: AnsenWen
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
    os.system("cd $SCS_PATH/jobs; scp -P 3600 spark/user_info_all_spark.py hadoop@192.168.0.94:/data/workspace/")

if __name__ == '__main__':

    send_file()

    s = ShellExec()
    out, err = s.execute(
        "cd /data/workspace; "
        "source ~/.bash_profile; "
        "spark-submit --master yarn-client --executor-memory 4G --num-executors 16  --py-files lib.zip user_info_all_spark.py " )
    print out, err





