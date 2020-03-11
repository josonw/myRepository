#! /usr/local/python272/bin/python
# coding: utf-8

from sys import path

import datetime
import paramiko
import time
import psycopg2
import os
import sys
import re

path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from libs.DB_Mysql import Connection
from libs import DB_PostgreSQL as pgdb

from PublicFunc import PublicFunc
from BaseStat import mysql_db
from dc_scs_jobs import config


class SyncTableInfo(object):

    def __init__(self, pg_table_name):
        self._connect_mysql()
        self.pg_table = self._get_table_info(pg_table_name)

    def _connect_mysql(self):
        self.mdb = mysql_db

    def _get_table_info(self, pg_table_name):
        sql = " select * from hive_pg where pg_tbl_name = '%s' " % pg_table_name
        res = self.mdb.getOne(sql)
        if not res:
            raise TableExceprtion('Table is not in mysql config table: `hive_pg`')
        return res

    def get(self, field):
        if self.pg_table:
            return self.pg_table.get(field, None)
        else:
            return None


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


class Sync(object):

    def __init__(self, sync_type='export', table=None):
        self.sync_type = sync_type
        self._get_db_ip(self.sync_type)
        if table:
            self._get_table_info(table)

        self.sqoop_export_cmd = ("sqoop export "
                 "-D mapreduce.job.queuename=sqoop "
                 "--connect jdbc:postgresql://%(ip)s/%(db_name)s "
                 "--username %(user)s "
                 "-m 1 "
                 "--password %(password)s "
                 "--staging-table %(staging_table)s "
                 "--clear-staging-table "
                 "--export-dir %(dir)s "
                 "--table %(table)s "
                 "--input-null-string '\\\\N' "
                 "--input-null-non-string '\\\\N' "
                 "--fields-terminated-by '\001' "
                 "-- --schema %(schema)s")

        self.sqoop_import_cmd = ("sqoop import "
                    "-D mapreduce.job.queuename=sqoop "
                    "--connect jdbc:postgresql://%(ip)s/%(db_name)s "
                    "--username %(user)s "
                    "-m 1 "
                    "--password %(password)s "
                    "--table %(table)s "
                    "--columns %(fieldstr)s "
                    "--target-dir %(dir)s "
                    "--delete-target-dir "
                    "--null-string '\\\\N' "
                    "--null-non-string '\\\\N' "
                    "--fields-terminated-by '\001' "
                    "-- --schema %(schema)s")

    def _get_table_info(self, pg_table):
        s = SyncTableInfo(pg_table)
        self.pg_tbl_name = s.get('pg_tbl_name')
        self.pg_schema = s.get('pg_schema')
        self.pg_partition_field = s.get('pg_partition_field')
        self.hive_db = s.get('hive_db')
        self.hive_dir = s.get('hive_dir')
        self.date_range = s.get('date_range')
        self.fieldstr = self.get_table_fields(self.pg_schema,self.pg_tbl_name)

    def _get_db_ip(self, sync_type):
        # data后台切换到集群时以注释下面行
        # self.DEST_HOST = config.PG_DB_HOST
        self.DEST_HOST = config.PGCLUSTER_HOST if sync_type == "import" else config.PG_DB_HOST
        print "pg host is:%s"%self.DEST_HOST
        self.pg_db = pgdb.Connection(host  = self.DEST_HOST,
                            database  = config.PG_DB_NAME,
                            user      = config.PG_DB_USER,
                            password  = config.PG_DB_PSWD)

    def get_table_fields(self,schema,tbl_name):
        sql = """select table_schema,table_name ,column_name,ordinal_position
                 from INFORMATION_SCHEMA.COLUMNS
                 where table_schema='%s' and table_name='%s' order by ordinal_position asc
              """%(schema,tbl_name)
        data=self.pg_db.query(sql)
        fieldstr = ",".join([item['column_name'] for item in data])

        return fieldstr

    def create_pg_staging(self, table_name):
        table_name_detail = self.pg_schema + '.' + table_name
        sql = "select * into %s_staging from %s where 1<>1" % (table_name_detail, table_name_detail)
        try:
            self.pg_db.execute(sql)
        except:
            pass

    def get_pg_del_sql(self, table_name, dates):
        """
        table_name: pg中要删除指定日期的表名
        """

        sqls = []

        for date in dates:
            sql = """ delete from %s.%s where %s >= DATE'%s' and %s < DATE'%s' + interval '1 day' """ % (
                self.pg_schema, table_name, self.pg_partition_field, date, self.pg_partition_field, date)
            sqls.append(sql)

        return tuple(sqls)

    def _get_sqoop_dict(self, staging_table, table, tbl_dir, schema):
        sqoop_dict = {"staging_table" : staging_table,
                              "table" : table,
                                "dir" : tbl_dir,
                           "fieldstr" : self.fieldstr,
                             "schema" : schema,
                                 'ip' : self.DEST_HOST,
                            'db_name' : config.PG_DB_NAME,
                               'user' : config.PG_DB_USER,
                           'password' : config.PG_DB_PSWD}
        return sqoop_dict

    def get_sqoop_export_cmd(self, table_name, dates):
        sqoop_cmd = self.sqoop_export_cmd

        cmds = []
        staging_table = table_name + '_staging'

        # 一次同步7天以上的用户改用同步dt='3000-01-01'分区,加快同步速度
        if len(dates)>=7:
            tbl_dir = self.hive_dir + '/dt=3000-01-01'
            cmd = sqoop_cmd % self._get_sqoop_dict(staging_table,table_name,tbl_dir,self.pg_schema)
            cmds.append(cmd)
        else:
            for date in dates:
                tbl_dir = self.hive_dir + '/dt=%s' % (date)
                cmd = sqoop_cmd % self._get_sqoop_dict(staging_table,table_name,tbl_dir,self.pg_schema)
                cmds.append(cmd)

        return cmds

    def get_sqoop_import_all_cmd(self, table_name):
        sqoop_cmd = self.sqoop_import_cmd

        cmds = []
        tbl_dir = self.hive_dir
        cmd = sqoop_cmd % self._get_sqoop_dict('', table_name, tbl_dir,self.pg_schema)
        cmds.append(cmd)
        return cmds

    def execute(self, table_name=None, date=None):
        self._ensure(table_name)
        self.date = date
        if self.sync_type == "export":
            self.create_pg_staging(table_name)

        sqls = []
        cmds = []

        if self.sync_type == 'import':
            cmds = self.get_sqoop_import_all_cmd(self.pg_tbl_name)
        else:
            dates = self._get_sync_days(self.date,self.date_range)
            sqls = self.get_pg_del_sql(self.pg_tbl_name, dates)
            cmds = self.get_sqoop_export_cmd(self.pg_tbl_name, dates)

        return self._execute(sqls, cmds)

    def _execute(self, sqls, cmds):
        s = ShellExec()

        for sql in sqls:
            print sql
            self.pg_db.execute(sql)

        for cmd in cmds:
            print cmd
            out, err = s.execute(cmd)
            if err and 'ERROR' in err:
                print err
                return cmd, err

        s.execute("rm -f /home/hadoop/*.java")
        return None, None

    def _get_sync_days(self,abs_date,date):
        dates = []

        pattern = re.compile(r'\[(\-)?\d+,(\-)?\d+\]')
        match = pattern.search(date)
        if match:
            date_arr = match.group().replace("[","").replace("]","").split(",");
            for num in range(int(date_arr[0]),int(date_arr[1])+1):
                date = get_date(abs_date,num)
                dates.append(PublicFunc.add_days(date, 0))
            return dates

        pattern = re.compile(r'{(\S+)(,\S+)*}')
        match = pattern.search(date)
        if match:
            date_arr = match.group().replace("{","").replace("}","").split(",");
            dates_dict = PublicFunc.date_define(get_date(abs_date,0))
            for date_val in date_arr:
                dates.append(dates_dict.get(date_val))
            return dates

        date = get_date(abs_date,int(date))
        dates.append(PublicFunc.add_days(date, 0))
        return dates

    def _ensure(self, table_name):
        if table_name:
            self._get_table_info(table_name)
        else:
            raise TableException('table not given')


class TableExceprtion(Exception):
    def __init__(self, arg):
        super(TableExceprtion, self).__init__()
        self.arg = arg

    def __str__(self):
        return self.arg

def get_date(abs_date_str=None,n=0):
    abs_date = None
    if abs_date_str:
        abs_date = datetime.datetime.strptime(abs_date_str, "%Y-%m-%d")
    else:
        abs_date = datetime.date.today()
        n = -1

    deltadays = datetime.timedelta(days=n)
    date = abs_date + deltadays
    return date.strftime("%Y-%m-%d")


if __name__ == '__main__':

    pass
