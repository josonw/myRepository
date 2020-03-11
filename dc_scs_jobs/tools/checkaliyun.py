# -*- coding: utf-8 -*-
'''
每天20.30分检测一下与阿里云mysql能不能连通，主要是付费的
'''
import sys, os
reload(sys)
sys.setdefaultencoding( "utf-8" )

dc_path = os.getenv('DC_SERVER_PATH')
from sys import path
path.append( dc_path )

from dc_scs_jobs import config
from libs.DB_Mysql import Connection as m_db
from libs.warning_way import send_sms
import traceback
import paramiko
import datetime

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
        self.ssh.connect(self.host, self.port, self.user, key_filename=self.pkey)

    def execute(self, cmd):
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        return stdout.read(), stderr.read()

def check_aliyunmysql():

    print "check aliyun mysql datetime:", datetime.datetime.now()
    s = ShellExec()
    mdb_scs = m_db(config.DB_HOST, config.DB_NAME, user=config.DB_USER, password=config.DB_PSWD)
    users = mdb_scs.query("SELECT * FROM loop_member")
    users_list = [item['user'] for item in users]
    mysql_is_ok = False
    try:
        cmd = """mysql -u{user} -h{host} -P{port} -p{passwd} {db_name} -e "show tables" """
        cmd = cmd.format(user=config.MYSQL_DB_SHUSER, host=config.MYSQL_DB_SHALIYUN, port=config.MYSQL_DB_SHPORT, passwd=config.MYSQL_DB_SHPSWD, db_name=config.MYSQL_DB_SHNAME)
        stdout, stderr = s.execute(cmd)
        print "stdout:", stdout
        print "stderr:", stderr
        if stdout:   #标准输出不为空
            if not stderr and not stderr.isspace():  #错误输出为空,说明没有错误，连接OK
                mysql_is_ok = True
    except Exception as e:
        print "check aliyun mysql fail", traceback.format_exc()
        mysql_is_ok = False

    if not mysql_is_ok:
        content = "阿里云MySQL连接测试失败，cmd:{cmd}".format(cmd=cmd)
    else:
        content = "阿里云MySQL连接测试成功，cmd:{cmd}".format(cmd=cmd)

    print content, datetime.datetime.now()
    if not mysql_is_ok:
        send_sms(users_list, "阿里云MySQL连接测试失败", priority=8)
    else:
        send_sms(users_list, "阿里云MySQL连接测试成功")



if __name__ ==  '__main__':

    check_aliyunmysql()

