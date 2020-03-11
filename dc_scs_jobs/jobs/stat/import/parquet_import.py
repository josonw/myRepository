# -*- coding: utf-8 -*-
"""
Created on Thu May 10 14:40:48 2018

@author: VasiliShi
"""
import json
import os
import datetime
import sys
from sys import path
path.append('%s/service' % os.getenv('SCS_PATH'))
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStat import BaseStat
from zookeeperhandler import zk
def type_map(typee):
    type_map={'long':'BIGINT','string':'STRING','map':'MAP'}
    return type_map.get(typee,typee)
class CreateTable(object):
    def __init__(self,tb_name):
        self.tb_name = tb_name
        s = zk.get("/bylog/schemas/parquet/%s"%tb_name)
        self.dicts = json.loads(s)

    def table_fields(self):
#        tb_name = self.dicts['name']
        json_fields = self.dicts['fields']
        tb_fields = [] #fields in sql
        fields = [] #columns
        dt_idx = None
        for i,field in enumerate(json_fields):
            name = field['name']
            if name == 'dt':
                dt_idx = i
            fields.append(name)
            typee = field['type']
            if isinstance(typee,dict):
                typee = '{}<STRING,{}>'.format(type_map(typee['type']),type_map(typee['values']))
            elif isinstance(typee,list):
                typee = typee[1] if typee[1] != 'null' else typee[0]
            typee = type_map(typee)
            if 'comment' in field.keys():
                one = '`{}` {} COMMENT \'{}\''.format(name,typee,field['comment'])
            else:
                one = '`{}` {}'.format(name,typee)
            tb_fields.append(one)#
        return tb_fields,fields,dt_idx

class ParquetImport(BaseStat):
    def __init__(self,tb_name,stat_date,eid=0):
        BaseStat.__init__(self,stat_date,eid)
        self.tb_name = tb_name
        self.get_params()
    def get_paras(self):
        """Necessary:just to Override the method in base class"""
        pass
    def get_params(self):
        ct = CreateTable(self.tb_name)
        tb_fields,fields,dt_idx = ct.table_fields()
        sql_fields = ",\n".join(tb_fields)
        fields.pop(dt_idx) #del dt column
        columns = ",\n".join(fields)
        dt_column = tb_fields.pop(dt_idx) #del dt column
        sql_fields_stg = ",\n".join(tb_fields)
        self.param_dict = {
                'tb_name':self.tb_name,
                'sql_fields':sql_fields,
                'columns':columns,
                'sql_fields_stg':sql_fields_stg,
                'numdate':self.hql_dict['ld_end'].replace('-',''),
                'yesterday':self.hql_dict['ld_begin'].replace('-',''),
                'numdate3ago':self.hql_dict.get('ld_3dayago').replace('-',''),
                'ld_end':self.hql_dict['ld_end'],
                'ld_begin':self.hql_dict['ld_begin']
            }
    def create_tab(self):
        hql = """
        CREATE TABLE if not exists today.parquet_%(tb_name)s_%(numdate)s (
            %(sql_fields)s
        )
        STORED AS PARQUET
        LOCATION '/dw/today/parquet_%(tb_name)s/dt=%(ld_end)s';
        """% self.param_dict
        res = self.hq.exe_sql(hql)

        hql = """
        CREATE EXTERNAL TABLE if not exists stage.parquet_%(tb_name)s_stg (
            %(sql_fields_stg)s
        )
        PARTITIONED BY (
            `dt` string)
        STORED AS PARQUET
        LOCATION '/dw/stage/parquet_%(tb_name)s';
        """%self.param_dict
        res = self.hq.exe_sql(hql)
        return res
    def stat(self):
        hql = """
        INSERT OVERWRITE TABLE stage.parquet_%(tb_name)s_stg PARTITION (dt='%(ld_begin)s')
        SELECT
            %(columns)s
        FROM today.parquet_%(tb_name)s_%(yesterday)s limit  1000000000;
        DROP TABLE IF EXISTS today.parquet_%(tb_name)s_%(numdate3ago)s ;
        """ % self.param_dict
        res = self.hq.exe_sql(hql)
        return res

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception('must input tb_name')
    elif len(sys.argv) == 2:
        tb_name = sys.argv[1]
        stat_date= datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
        parquet = ParquetImport(tb_name,stat_date)
        parquet()
    elif len(sys.argv) >= 3:
        tb_name = sys.argv[1]
        stat_date = sys.argv[2]
        try:
            datetime.datetime.strptime(stat_date, "%Y-%m-%d")
        except ValueError as e:
            print(e)

        parquet = ParquetImport(tb_name,stat_date)
        parquet()
