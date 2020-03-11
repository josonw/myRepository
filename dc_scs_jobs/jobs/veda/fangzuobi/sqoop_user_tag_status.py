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
import config
from libs.DB_Mysql import Connection


mysql_db = Connection(host      = config.MYSQL_DFQP_HOST,
                      database  = config.MYSQL_DFQP_DBNAME,
                      user      = config.MYSQL_DFQP_USER,
                      password  = config.MYSQL_DFQP_PSWD)


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
    sql="INSERT INTO user_tag_status(dt,flag) VALUES ('%s',1) ON DUPLICATE KEY UPDATE flag=VALUES(flag);"%date

    mysql_db.execute(sql)
    print "用户标签更新成功",sql
