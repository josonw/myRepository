#! /usr/local/python272/bin/python
# coding: utf-8

from sys import path

import datetime
import time
import psycopg2
import os
import sys
import re
import paramiko

path.append('%s/' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('DC_SERVER_PATH'))

from libs.DB_Mysql import Connection

from PublicFunc import PublicFunc
from BaseStat import  mysql_db
import config
from libs import DB_PostgreSQL as pgdb

pg_db = pgdb.Connection(host      = config.PGCLUSTER_HOST,
                        database  = config.PG_DB_NAME,
                        user      = config.PG_SCS_USER,
                        password  = config.PG_SCS_PSWD)

class ShellExec(object):

    host = config.NAME_NODE_SSH_IP
    port = config.NAME_NODE_SSH_PORT
    user = config.NAME_NODE_SSH_USER
    pkey = config.NAME_NODE_SSH_PKEY

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

class SyncTableInfo(object):

    def __init__(self, pg_table_name):
        self._connect_mysql()
        self.pg_table = self._get_table_info(pg_table_name)

    def _connect_mysql(self):
        self.mdb = mysql_db

    def _get_table_info(self, pg_table_name):
        sql = """ select pg_tbl_name,pg_schema, partitioned, pg_partition_field,hive_db,hive_dir,date_range, '1' type
                  from hive_pg where pg_tbl_name = '%s'
                  union all
                  select tbl_name pg_tbl_name, `schema` pg_schema, partitioned, partition_field pg_partition_field,
                        `schema` hive_db, concat('/dw/',`schema`,'/',tbl_name) hive_dir, flag date_range, '0' type
                  from sync_hive_pg where tbl_name = '%s' """% (pg_table_name,pg_table_name)

        res = self.mdb.getOne(sql)
        if not res:
            raise TableExceprtion('Table is not in mysql config table: `hive_pg` and `sync_hive_pg`')
        return res

    def get(self, field):
        if self.pg_table:
            return self.pg_table.get(field, None)
        else:
            return None

class Sync(object):

    def __init__(self, sync_type='export', table=None):
        self.sync_type = sync_type
        if table:
            self._get_table_info(table)

        self.create_readable_external_table_sql = """
        set gp_autostats_mode='on_change';
        set gp_autostats_on_change_threshold=10000;
        drop external table if exists _%(pg_schema)s_%(pg_tbl_name)s_%(date)s;
        create external table  _%(pg_schema)s_%(pg_tbl_name)s_%(date)s
        (%(hfields)s)
        location(%(files_dir)s)
        format 'text' (delimiter E'\x01' escape 'OFF');
        insert into %(pg_schema)s.%(pg_tbl_name)s select %(pfields)s from _%(pg_schema)s_%(pg_tbl_name)s_%(date)s;
        drop external table if exists _%(pg_schema)s_%(pg_tbl_name)s_%(date)s"""

        self.create_writable_external_table_sql="""
        drop external table if exists _%(pg_schema)s_%(pg_tbl_name)s;
        CREATE WRITABLE EXTERNAL TABLE _%(pg_schema)s_%(pg_tbl_name)s
        (%(hfields)s)
        location(%(files_dir)s)
        format 'text' (delimiter E'\x01' escape 'OFF');
        insert into _%(pg_schema)s_%(pg_tbl_name)s select %(pfields)s from %(pg_schema)s.%(pg_tbl_name)s
        """


    def _get_table_info(self,pg_table):
        s = SyncTableInfo(pg_table)
        self.pg_tbl_name = s.get('pg_tbl_name')
        self.pg_schema = s.get('pg_schema')
        self.partitioned = s.get('partitioned')
        self.pg_partition_field = s.get('pg_partition_field')
        self.hive_db = s.get('hive_db')
        self.hive_dir = s.get('hive_dir')
        self.date_range = s.get('date_range')
        self.namenodehost = config.NAME_NODE_HOST
        self.type = s.get('type')


    def get_pg_fieldstr(self, partitioned, dates=None):
        if not dates:
            dates = [self.date]

        sqldata = {'schema':self.pg_schema,'tablenum':self.pg_tbl_name}

        if partitioned==1:
            sqldata = self.create_pgcluster_partitiontable('child', self.pg_tbl_name, dates)

        sql = """select column_name,data_type
                   from information_schema.columns
                  where table_schema='%(schema)s' and table_name='%(tablenum)s' order by ordinal_position asc """%sqldata

        tableinfo = pg_db.query(sql)

        fieldstr = ','.join(["%s %s\n"%(item['column_name'],item['data_type'])  for item in tableinfo])
        fieldstr_notype = ','.join(["%s\n"%(item['column_name'])  for item in tableinfo])
        return fieldstr,fieldstr_notype


    def create_pgcluster_partitiontable(self, pg_schema, table_name, dates):
        sqldata={}
        sqldata['date'] = dates[0]
        sqldata['datenum'] = date2num(dates[0])
        sqldata['date1day'] = PublicFunc.add_days(dates[0], 1)
        sqldata['schema'] = pg_schema
        sqldata['table_name'] = table_name
        sqldata['tablenum'] = '%s_%s'%(table_name, sqldata['datenum'])
        # 父表的schema
        sqldata['pg_schema'] = self.pg_schema

        sql = """select column_name,data_type
                   from information_schema.columns
                  where table_schema='%(schema)s' and table_name='%(tablenum)s' """%sqldata
        tableinfo = pg_db.query(sql)

        if not tableinfo:
            csql="""create table %(schema)s.%(tablenum)s
                   (CONSTRAINT %(tablenum)s_fdate_check CHECK (((fdate >= '%(date)s'::date) AND (fdate < '%(date1day)s'::date))) ) INHERITS (%(pg_schema)s.%(table_name)s);
                   CREATE INDEX %(tablenum)s_fdate_index on %(schema)s.%(tablenum)s (fdate);
                   ALTER TABLE %(schema)s.%(tablenum)s OWNER TO analysis
            """%sqldata
            print csql
            pg_db.execute(csql)

        self.pg_tbl_name = sqldata['tablenum']
        self.pg_schema = sqldata['schema']

        return sqldata

    def get_pg_del_sql(self, table_name, dates, _type):
        """
        table_name: pg中要删除指定日期的表名
        """

        sqls = []
        if _type=="line":

            sql = """ delete from %s.%s where %s >= DATE'%s' and %s < DATE'%s' + interval '1 day' """ % (
                    self.pg_schema, table_name, self.pg_partition_field, dates[0], self.pg_partition_field, dates[-1])
            sqls.append(sql)

        else:
            for date in dates:
                sql = """ delete from %s.%s where %s >= DATE'%s' and %s < DATE'%s' + interval '1 day' """ % (
                    self.pg_schema, table_name, self.pg_partition_field, date, self.pg_partition_field, date)
                sqls.append(sql)

        return tuple(sqls)

    def get_pg_trucate_sql(self, table_name):
        """
        truncate pg集群中的指定表
        """
        sqls = []
        sql = """truncate %s.%s"""%(self.pg_schema,table_name)
        sqls.append(sql)
        return tuple(sqls)

    def hive_concat(self, table_name, dates):
        shell = ShellExec()

        args = {
            "table": table_name,
            "date": date2num(self.date),
            "dates": ",".join(["'" + date + "'" for date in dates])
        }
        # 该脚本已绑定在老的同步pg脚本stat/sqoop_export.py之后，可以省略执行合并到一个分区
        # hive_cmd = ("source ~/.bash_profile; "
        #             "hive -e \"use analysis; insert overwrite directory "
        #             "'/dw/analysis/%(table)s/dt=%(date)s' "
        #             "select * from %(table)s "
        #             "where cast(dt as string) in (%(dates)s)\" ") % args

        # out, err = shell.execute(hive_cmd)

        # if err and ('ERROR' in err or 'Exception' in err):
        #     print hive_cmd
        #     raise Exception(err)

    def _get_sql_dict(self, hfields, pfields, table, hive_dir, pg_schema, date):
        sql_dict = {         "hfields": hfields,
                             "pfields": pfields,
                        "pg_tbl_name" : table,
                          "files_dir" : hive_dir,
                          "pg_schema" : pg_schema,
                          "date": date.replace('-', '')
                    }
        return sql_dict

    def get_create_extable_export_sql(self, table_name, dates):
        create_tblsql = self.create_readable_external_table_sql
        hfields, pfields = self.get_pg_fieldstr(self.partitioned, dates)
        # 一次同步7天以上的用户改用同步dt='3000-01-01'分区,加快同步速度
        # gphdfs不支持多个url,
        if len(dates)>=2 and self.type=='1':
            tbl_dir = "'%s%s%s%s'"%("gphdfs://",self.namenodehost, self.hive_dir, '/dt=3000-01-01/*')
            pfields='*'

        elif len(dates)>=2 and self.type=='0':
            tbl_dir = "'%s%s%s%s'"%("gphdfs://",self.namenodehost, self.hive_dir, '/dt=%s/*'%(date2num(self.date)))
            self.hive_concat(table_name, dates)
            hfields = "%s%s"%(hfields,",dt date\n")

        else:
            tbl_dir = "'%s%s%s%s'"%("gphdfs://",self.namenodehost, self.hive_dir, '/dt=%s/*'%dates[0])
            pfields='*'

        cmd = create_tblsql % self._get_sql_dict(hfields,pfields,self.pg_tbl_name,tbl_dir,self.pg_schema, self.date)

        return cmd

    def get_create_sql_cmd(self, table_name):
        create_tblsql = self.create_readable_external_table_sql
        hfields, pfields = self.get_pg_fieldstr(self.partitioned)

        tbl_dir = "'%s%s%s%s'"%("gphdfs://",self.namenodehost, self.hive_dir, '/*')
        cmd = create_tblsql % self._get_sql_dict(hfields,'*', self.pg_tbl_name, tbl_dir, self.pg_schema, self.date)
        return cmd

    def get_create_extable_import_sql(self, table_name):
        create_tblsql = self.create_writable_external_table_sql
        hfields, pfields = self.get_pg_fieldstr(self.partitioned)

        tbl_dir = "'%s%s%s'"%("gphdfs://",self.namenodehost, self.hive_dir)
        cmd = create_tblsql % self._get_sql_dict(hfields,'*', table_name, tbl_dir,self.pg_schema, self.date)
        return cmd

    def execute(self, table_name=None, date=None):
        self._ensure(table_name)
        self.date = date
        sqls = []
        cmds = []

        if self.sync_type == 'import':
            cmds = self.get_create_extable_import_sql(self.pg_tbl_name)

        else:
            if self.date_range=='ALL':
                sqls = self.get_pg_trucate_sql(self.pg_tbl_name)
                cmds = self.get_create_sql_cmd(self.pg_tbl_name)

            else:
                if self.type=='1':
                    dates, _type = self._get_sync_days2(self.date,self.date_range)
                else:
                    dates, _type = self._get_sync_days1(self.date)

                sqls = self.get_pg_del_sql(self.pg_tbl_name, dates, _type)
                cmds = self.get_create_extable_export_sql(self.pg_tbl_name, dates)

        return self._execute(sqls, cmds)

    def _execute(self, sqls, cmds):
        for sql in sqls:
            print sql
            pg_db.execute(sql)

        for sql in cmds.split(';'):
            print sql
            pg_db.execute(sql)

        return None, None

    def _get_sync_days2(self,abs_date,date):
        dates = []

        pattern = re.compile(r'\[(\-)?\d+,(\-)?\d+\]')
        match = pattern.search(date)
        if match:
            date_arr = match.group().replace("[","").replace("]","").split(",");
            for num in range(int(date_arr[0]),int(date_arr[1])+1):
                date = get_date(abs_date,num)
                dates.append(PublicFunc.add_days(date, 0))
            return dates,'line'

        pattern = re.compile(r'{(\S+)(,\S+)*}')
        match = pattern.search(date)
        if match:
            date_arr = match.group().replace("{","").replace("}","").split(",");
            dates_dict = PublicFunc.date_define(get_date(abs_date,0))
            for date_val in date_arr:
                dates.append(dates_dict.get(date_val))
            return dates,'point'

        date = get_date(abs_date,int(date))
        dates.append(PublicFunc.add_days(date, 0))
        return dates,'point'

    def _get_sync_days1(self, date):
        if not date:
            date = get_date(-1)

        dates = []

        if self.date_range == 'LM':
            dates.append(PublicFunc.trunc(PublicFunc.add_months(date, -1), 'MM'))
            return dates, 'point'
        elif self.date_range.startswith('MM'):
            if self.date_range == 'MM':
                dates.append(PublicFunc.trunc(date, 'MM'))
            elif self.date_range == 'MM:0-3':
                dates.append(PublicFunc.add_months(PublicFunc.trunc(date, 'MM'), -3))
                dates.append(PublicFunc.add_months(PublicFunc.trunc(date, 'MM'), -2))
                dates.append(PublicFunc.add_months(PublicFunc.trunc(date, 'MM'), -1))
                dates.append(PublicFunc.add_months(PublicFunc.trunc(date, 'MM'), 0))
            return dates, 'point'
        elif self.date_range.startswith('IW'):
            if self.date_range == 'IW':
                dates.append(PublicFunc.trunc(date, 'IW'))
            elif self.date_range == 'IW:0-8':
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
        elif ',' in self.date_range:
            for day in self.date_range.strip().split(','):
                dates.append(PublicFunc.add_days(date, 0 - int(day)))
            return dates, 'point'
        elif '-' in self.date_range:
            e_day, s_day = self.date_range.strip().split('-')
            endday = PublicFunc.add_days(date, 0 - int(s_day))
            staday = PublicFunc.add_days(date, 0 - int(e_day))
            dates = AutoCompleDate(endday, staday)
            return dates, 'line'
        else:
            dates.append(PublicFunc.add_days(date, 0 - int(self.date_range)))
            return dates, 'point'


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

def date2num(date_str):
    return date_str.replace('-', '')

def AutoCompleDate(sdate, edate):
    """返回开始日期到结束日期的的所有日期
    """
    s_date = datetime.datetime.strptime(sdate,'%Y-%m-%d')
    e_date = datetime.datetime.strptime(edate,'%Y-%m-%d')
    days = (e_date - s_date).days
    dates = [ (s_date + datetime.timedelta(days = d)).strftime('%Y-%m-%d') for d in xrange(days+1) ]
    return dates

if __name__ == '__main__':

    pass
