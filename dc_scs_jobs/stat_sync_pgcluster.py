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
from BaseStat import mysql_db
import config
from libs import DB_PostgreSQL as pgdb

pg_db = pgdb.Connection(host      = config.PGCLUSTER_HOST,
                        database  = config.PG_DB_NAME,
                        user      = config.PG_DB_USER,
                        password  = config.PG_DB_PSWD)


class SyncTableInfo(object):

    def __init__(self, table_name):
        self._connect_mysql()
        self.table_info = self._get_table_info(table_name)

    def _connect_mysql(self):
        self.mdb = mysql_db

    def _get_table_info(self, table_name):
        sql = " select * from sync_hive_pg where tbl_name = '%s' " % table_name
        res = self.mdb.getOne(sql)
        if not res:
            raise TableExceprtion('table is not in mysql config table: `sync_hive_pg`')
        return res

    def get(self, field):
        if self.table_info:
            return self.table_info.get(field, None)
        else:
            return None

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


class Sync(object):

    def __init__(self, sync_type='export', table=None):
        self.sync_type = sync_type
        self.table = table
        if self.table:
            self._get_table_info()


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
                    "--target-dir %(dir)s "
                    "--delete-target-dir "
                    "--null-string '\\\\N' "
                    "--null-non-string '\\\\N' "
                    "--fields-terminated-by '\001' ")

    def _get_table_info(self):
        s = SyncTableInfo(self.table)
        self.tbl_name = s.get('tbl_name')
        self.schema = s.get('schema')
        self.partitioned = s.get('partitioned')
        self.partition_field = s.get('partition_field')
        self.tbl_type = s.get('tbl_type')
        self.flag = s.get('flag')

    def create_pg_staging(self, table_name):
        sql = "select * into %s_staging from %s where 1<>1" % (table_name, table_name)
        try:
            pg_db.execute(sql)
        except:
            pass

    def create_pgcluster_partitiontable(self, schema, table_name, dates):
        sqldata={}
        sqldata['date'] = dates[0]
        sqldata['datenum'] = date2num(dates[0])
        sqldata['date1day'] = PublicFunc.add_days(dates[0], 1)
        sqldata['schema'] = schema
        sqldata['table_name'] = table_name
        sqldata['tablenum'] = '%s_%s'%(table_name, sqldata['datenum'])

        sql = """select column_name,data_type
                   from information_schema.columns
                  where table_schema='%(schema)s' and table_name='%(tablenum)s' """%sqldata
        tableinfo = pg_db.query(sql)
        if not tableinfo:
            csql="""create table %(schema)s.%(tablenum)s
                   (CONSTRAINT %(tablenum)s_fdate_check CHECK (((fdate >= '%(date)s'::date) AND (fdate < '%(date1day)s'::date))) ) INHERITS (analysis.%(table_name)s);
                   CREATE INDEX %(tablenum)s_fdate_index on %(schema)s.%(tablenum)s (fdate);
                   ALTER TABLE %(schema)s.%(tablenum)s OWNER TO analysis
            """%sqldata
            print csql
            pg_db.execute(csql)

    def get_pg_del_sql(self, table_name, dates, _type='point'):
        """
        table_name: pg中要删除指定日期的表名

        dates: 时间列表，元素为 ‘2014-01-01’ 类型的日期字符串

        type: line 时间段，dates中包含两个元素，dates[0] 起始时间，dates[-1] 终止时间
              point 时间点，dates中每个元素为单独的一天
        """

        sqls = []

        if _type == 'line':
            sql = """
                  delete from %s.%s
                   where %s >= DATE'%s'
                     and %s < DATE'%s'
                  """ % (self.schema,
                         table_name,
                         self.partition_field, dates[0],
                         self.partition_field, dates[-1])
            sqls.append(sql)

        elif _type == 'point':
            for date in dates:
                sql = """ delete from %s.%s where %s >= DATE'%s' and %s < DATE'%s' + interval '1 day' """ % (
                    self.schema, table_name, self.partition_field, date, self.partition_field, date)
                sqls.append(sql)
        else:
            pass

        return tuple(sqls)

    def get_pg_trucate_sql(self, table_name, dates=None, _type='point'):
        """
        table_name: pg中要删除指定日期的表名

        dates: 时间列表，元素为 ‘2014-01-01’ 类型的日期字符串
        """

        sqls = []

        if not dates:
            sql = """ delete from %s.%s """ % (self.schema, table_name)
            sqls.append(sql)
            return tuple(sqls)

        if _type == 'line':
            sync_dates = get_day_list(dates[0], dates[-1])
        elif _type == 'point':
            sync_dates = dates

        if self.schema == 'analysis':
            schema = 'child'
        else:
            schema = self.schema

        if schema == 'child':
            # pg集群的分区表触发器函数不支持
            self.create_pgcluster_partitiontable(schema, table_name, dates)

        for date in sync_dates:
            sql = """ delete from %s.%s_%s """ % (schema, table_name, date2num(date))
            sqls.append(sql)

        return tuple(sqls)

    def get_sqoop_export_cmd(self, table_name, dates, _type='point'):
        sqoop_cmd = self.sqoop_export_cmd

        cmds = []
        staging_table = table_name + '_staging'
        if _type == 'line':
            sync_dates = get_day_list(dates[0], dates[-1])
        elif _type == 'point':
            sync_dates = dates

        for date in sync_dates:
            tbl_dir = '/dw/analysis/%s/dt=%s' % (table_name, date)
            cmd = sqoop_cmd % self._get_sqoop_dict(staging_table, table_name, tbl_dir, self.schema)
            cmds.append(cmd)
        return cmds

    def _get_sqoop_dict(self, staging_table, table, tbl_dir, schema):
        sqoop_dict = {"staging_table" : staging_table,
                              "table" : table,
                                "dir" : tbl_dir,
                             "schema" : schema,
                                 'ip' : config.PGCLUSTER_IP,
                            'db_name' : config.PG_DB_NAME,
                               'user' : config.PG_DB_USER,
                           'password' : config.PG_DB_PSWD}
        return sqoop_dict

    def get_sqoop_export_sub_cmd(self, table_name, dates, _type='point'):
        sqoop_cmd = self.sqoop_export_cmd

        cmds = []
        staging_table = table_name + '_staging'

        if _type == 'point':
            sync_dates = dates
        elif _type == 'line':
            sync_dates = get_day_list(dates[0], dates[-1])

        if self.schema == 'analysis':
            schema = 'child'
        else:
            schema = self.schema
        for date in sync_dates:
            tbl_dir = '/dw/analysis/%s/dt=%s' % (table_name, date)
            table = table_name + '_' + date2num(date)
            cmd = sqoop_cmd % self._get_sqoop_dict(staging_table, table, tbl_dir, schema)
            cmds.append(cmd)
        return cmds


    def get_sqoop_export_all_cmd(self, table_name):
        sqoop_cmd = self.sqoop_export_cmd

        cmds = []
        staging_table = table_name + '_staging'
        tbl_dir = '/dw/analysis/%s' % table_name
        cmd = sqoop_cmd % self._get_sqoop_dict(staging_table, table_name, tbl_dir, self.schema)
        cmds.append(cmd)
        return cmds

    def get_sqoop_import_all_cmd(self, table_name):
        sqoop_cmd = self.sqoop_import_cmd

        cmds = []
        tbl_dir = '/dw/analysis/%s' % table_name
        cmd = sqoop_cmd % self._get_sqoop_dict('', table_name, tbl_dir, '')
        cmds.append(cmd)
        return cmds

    def hive_concat(self, table_name, dates, _type):
        shell = ShellExec()
        if _type == "line":
            concat_dates = get_day_list(dates[0], dates[-1])
        elif _type == "point":
            concat_dates = dates
        args = {
            "table": table_name,
            "date": date2num(self.date),
            "dates": ",".join(["'" + date + "'" for date in concat_dates])
        }
        hive_cmd = ("source ~/.bash_profile; "
                    "hive -e \"use analysis; insert overwrite directory "
                    "'/dw/analysis/%(table)s/dt=%(date)s' "
                    "select * from %(table)s "
                    "where cast(dt as string) in (%(dates)s)\" ") % args
        out, err = shell.execute(hive_cmd)

        if err and ('ERROR' in err or 'Exception' in err):
            print hive_cmd
            raise Exception(err)

    def execute(self, table_name=None, date=None):
        self._ensure(table_name)
        self.date = date
        if self.sync_type == "export":
            self.create_pg_staging(table_name)

        sqls = []
        cmds = []

        # 全表同步
        if self.flag == 'ALL':
            if self.sync_type == 'export':
                sqls = self.get_pg_trucate_sql(self.tbl_name)
                cmds = self.get_sqoop_export_all_cmd(self.tbl_name)
            elif self.sync_type == 'import':
                cmds = self.get_sqoop_import_all_cmd(self.tbl_name)
        # 分区同步
        else:
            dates, _type = self._get_sync_days(date)
            if self.partitioned == 0:
                sqls = self.get_pg_del_sql(self.tbl_name, dates, _type)
                if len(dates) > 1:
                    self.hive_concat(self.tbl_name, dates, _type)
                    dates = [date2num(date)]
                    _type = "point"
                cmds = self.get_sqoop_export_cmd(self.tbl_name, dates, _type)
            elif self.partitioned == 1:
                sqls = self.get_pg_trucate_sql(self.tbl_name, dates, _type)
                cmds = self.get_sqoop_export_sub_cmd(self.tbl_name, dates, _type)
        return self._execute(sqls, cmds)

    def _execute(self, sqls, cmds):
        s = ShellExec()

        for sql in sqls:
            print sql
            pg_db.execute(sql)

        for cmd in cmds:
            print cmd
            out, err = s.execute(cmd)
            if err and 'ERROR' in err:
                print err
                return cmd, err

        s.execute("rm -f /home/hadoop/*.java")
        return None, None

    def _get_sync_days(self, date):
        if not date:
            date = get_date(-1)

        dates = []

        if self.flag == 'LM':
            dates.append(PublicFunc.trunc(PublicFunc.add_months(date, -1), 'MM'))
            return dates, 'point'
        elif self.flag.startswith('MM'):
            if self.flag == 'MM':
                dates.append(PublicFunc.trunc(date, 'MM'))
            elif self.flag == 'MM:0-3':
                dates.append(PublicFunc.add_months(PublicFunc.trunc(date, 'MM'), -3))
                dates.append(PublicFunc.add_months(PublicFunc.trunc(date, 'MM'), -2))
                dates.append(PublicFunc.add_months(PublicFunc.trunc(date, 'MM'), -1))
                dates.append(PublicFunc.add_months(PublicFunc.trunc(date, 'MM'), 0))
            return dates, 'point'
        elif self.flag.startswith('IW'):
            if self.flag == 'IW':
                dates.append(PublicFunc.trunc(date, 'IW'))
            elif self.flag == 'IW:0-8':
                dates.append(PublicFunc.add_days(PublicFunc.trunc(date, 'IW'), -7*8))
                dates.append(PublicFunc.add_days(PublicFunc.trunc(date, 'IW'), -7*7))
                dates.append(PublicFunc.add_days(PublicFunc.trunc(date, 'IW'), -7*6))
                dates.append(PublicFunc.add_days(PublicFunc.trunc(date, 'IW'), -7*5))
                dates.append(PublicFunc.add_days(PublicFunc.trunc(date, 'IW'), -7*4))
                dates.append(PublicFunc.add_days(PublicFunc.trunc(date, 'IW'), -7*3))
                dates.append(PublicFunc.add_days(PublicFunc.trunc(date, 'IW'), -7*2))
                dates.append(PublicFunc.add_days(PublicFunc.trunc(date, 'IW'), -7*1))
                dates.append(PublicFunc.add_days(PublicFunc.trunc(date, 'IW'), -7*0))
            return dates, 'point'
        elif ',' in self.flag:
            for day in self.flag.strip().split(','):
                dates.append(PublicFunc.add_days(date, 0 - int(day)))
            return dates, 'point'
        elif '-' in self.flag:
            e_day, s_day = self.flag.strip().split('-')
            dates.append(PublicFunc.add_days(date, 0 - int(s_day)))
            dates.append(PublicFunc.add_days(date, 1 - int(e_day)))
            return dates, 'line'
        else:
            dates.append(PublicFunc.add_days(date, 0 - int(self.flag)))
            return dates, 'point'


    def _ensure(self, table_name):
        if table_name:
            self.table = table_name

        if self.table:
            self._get_table_info()
        else:
            raise TableException('table not given')


class TableExceprtion(Exception):
    def __init__(self, arg):
        super(TableExceprtion, self).__init__()
        self.arg = arg

    def __str__(self):
        return self.arg


def date2num(date_str):
    return date_str.replace('-', '')

def get_date(n=0):
    today = datetime.date.today()
    deltadays = datetime.timedelta(days=n)
    date = today + deltadays
    return date.strftime("%Y-%m-%d")

def get_day_list(start_date, end_date, step=1):
    try:
        s_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        e_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    except:
        print '日期格式错误'
        sys.exit(1)

    dates = []
    date = s_date
    dates.append(date.strftime("%Y-%m-%d"))
    while True:
        date += datetime.timedelta(days=step)
        if date >= e_date:
            break
        dates.append(date.strftime("%Y-%m-%d"))

    return dates


if __name__ == '__main__':
    # s = Sync(sys.argv[1])
    # s.execute(sys.argv[2])
    s = ShellExec()
    dates = get_day_list('2015-12-13', '2016-01-12')
    args = {
        "table": "user_act_retained_fct",
        "date": "2016-01-13",
        "dates": ",".join(["'" + date + "'" for date in dates])
    }
    hive_cmd = ("hive -e \"use analysis; insert overwrite directory "
                "'/dw/analysis/%(table)s/%(date)s' "
                "select * from %(table)s "
                "where cast(dt as string) in (%(dates)s)\" ") % args
    print hive_cmd
    s.execute(hive_cmd)
