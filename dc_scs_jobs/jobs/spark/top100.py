#! /usr/local/python272/bin/python
# coding: utf-8

from sys import path

import datetime
import paramiko


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

    def execute(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        return stdout.read(), stderr.read()


#  "spark-submit --master yarn --conf spark.port.maxRetries=50 --py-files lib.zip  top100.py"

if __name__ == '__main__':
    s = ShellExec()
    out, err = s.execute(
        "cd /data/workspace/top100; "
        "source ~/.bash_profile; "
        "spark-submit --driver-memory 3g --executor-memory 5g --num-executors 5 --driver-cores 8 --conf spark.port.maxRetries=50 --conf spark.dynamicAllocation.enabled=false top100.py"
        )
    print out, err
