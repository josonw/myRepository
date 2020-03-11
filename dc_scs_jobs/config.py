#-*- coding: UTF-8 -*-
import os, os.path

"""
报警短信列表
"""
CONTACTS_LIST_PHONE = ['18312562113', '18688757750', '15220289390','18588426005', '13725517185',
                       '13570819926', '13728038488', '13613003323']

CONTACTS_LIST_ENAME = ['HymanShi','WelkinChen','VasiliShi','SimonRen','LigaLi','MarkCai']
CONTACTS_REALTIME_ENAME = ['HymanShi','WelkinChen','GodmanWang']
CONTACTS_PAYAPI_ENAME = ['HymanShi','FelixSun']

DB_HOST = "10.30.101.125:3388"
DB_NAME = "byscs"
DB_USER = "byscs_user"
DB_PSWD = "BtpllrkvcCvyqiulozwc"


PG_DB_IP   = "10.30.101.240"
PG_DB_HOST = "10.30.101.240:5432"
PG_DB_NAME = "boyaadw"
PG_DB_USER = "analysis"
PG_DB_PSWD = "analysis"
PGCLUSTER_IP = "10.30.101.240"
PGCLUSTER_HOST = "10.30.101.240:5432"

PGCLUSTER_SZIP = "10.30.100.19"   #用于香港集群(大集群)向深圳集群(小集群)同步数据
PGCLUSTER_SZHOST = "10.30.100.19:5432"
#专用于集群任务账号,区别于PG_DB_USER,两个不同资源队列,降低集群负载
PG_SCS_USER='dcscs'
PG_SCS_PSWD='dcscs'

HADOOP_YARN_RES_IP = "10.30.101.94"  #Hadoop Yarn资源管理IP
HADOOP_MR_HIS_IP = "10.30.101.94"  #Hadoop MapReduce History Server IP

IMPALA_DB_IP   = "10.30.101.104"
IMPALA_DB_PORT = 21050

HIVE_DB_IP   = "10.30.101.216"
HIVE_DB_PORT = 10000
HIVE_DB_AUTH = "PLAIN"
HIVE_DB_USER = "hadoop"
HIVE_DB_PSWD = ""

HIVE_NODE_SSH_IP = '10.30.101.94'
HIVE_NODE_SSH_PORT = 3600
HIVE_NODE_SSH_USER = 'hadoop'
HIVE_NODE_SSH_PKEY = '/home/hadoop/.ssh/id_rsa'

NAME_NODE_SSH_IP = '10.30.101.92'
NAME_NODE_SSH_PORT = 3600
NAME_NODE_SSH_USER = 'hadoop'
NAME_NODE_SSH_PKEY = '/home/hadoop/.ssh/id_rsa'

NAME_NODE_HOST = "10.30.101.92:8020"

SPARK_DB_PORT = 10008

HMETA_DB_HOST = "10.30.101.216:3388"
HMETA_DB_NAME = "hive_meta_exp"
HMETA_DB_USER = "hive"
HMETA_DB_PSWD = "bydc_hive_sa"


MYSQL_DB_HOST = "192.168.0.125:3388"
MYSQL_DB_ALIYUN = "rm-uf6d0xgy2a572ebsgo.mysql.rds.aliyuncs.com"
#192.168.0.125:3388
#rm-uf6d0xgy2a572ebsgo.mysql.rds.aliyuncs.com
MYSQL_DB_NAME = "paycenter"
MYSQL_DB_USER = "dc_user"
MYSQL_DB_PSWD = "g0cmjeGkwtrx"

MYSQL_DB_SHALIYUN="rm-uf6d0xgy2a572ebsg.mysql.rds.aliyuncs.com"
MYSQL_DB_SHPORT = 3306
MYSQL_DB_SHNAME = "paycenter"
MYSQL_DB_SHUSER = "paycenter"
MYSQL_DB_SHPSWD = "V6pdajwAz4pTF9Wm"


PAY_DB_HOST = '192.168.0.121:3388'
PAY_DB_NAME = "paycenter"
PAY_DB_USER = "selectpay" #"stage"
PAY_DB_PSWD = "lfzezc0LbpwmhhZ9j_wsxmwr" #"stage"

#地方棋牌防作弊
MYSQL_DFQP_HOST = "rm-uf6221u1a0d9963kg.mysql.rds.aliyuncs.com:3306"
MYSQL_DFQP_DBNAME = "datacenter"
MYSQL_DFQP_USER = "dc_user"
MYSQL_DFQP_PSWD = "ZGF0YWNlbnRlcg==​"

# {'host':'192.168.0.94', 'port':10000, 'authMechanism':'PLAIN', 'user':'hadoop', 'password':'bydchadoop'}

LOGGING_FILE = 'tmp\\dc_query.log'

ADMINS_MAIL_LIST    = ['HymanShi@boyaa.com',]

