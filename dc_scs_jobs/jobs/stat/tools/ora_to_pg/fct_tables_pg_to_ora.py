#!/usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
from gevent import monkey;monkey.patch_all()

import os
import sys
import re
import datetime
import logging
import gevent
from gevent import Greenlet

import conf

class PGToOra(Greenlet):
    def __init__(self, dbmanage, ora_table_name, sdate=None, edate=None):
        Greenlet.__init__(self)

        self.dbmanage = dbmanage
        self.ora_table_name = ora_table_name
        self.sdate = sdate
        self.edate = edate
        self.table_name = self._get_table_name(ora_table_name)

    # 获取pg表名
    # pg的表只在analysis空间下所以过滤掉所有表空间前缀
    def _get_table_name(self, table_name):
        temp_table_name = table_name.replace('analysis.', '').replace('stage.', '')
        special_map = {'ddz_jinbi_fcnt':'ddz_jinbi_fct'}

        return special_map.get(temp_table_name, temp_table_name)


    def _get_pg_table_cols_str(self):
        sql = """ select relname,attname,typname FROM pg_class c,pg_attribute a,pg_type t
                   WHERE c.relname = '%s'
                     AND c.relnamespace = 16386
                     AND c.oid = attrelid AND atttypid = t.oid AND attnum > 0 """ % self.table_name
        datas, _ = self.dbmanage.query_pg_datas(sql)

        if not datas:
            raise Exception("table %s is not exist." % self.table_name)

        return ','.join( [ data[1] for data in datas ] )

    def _get_insert_values_str(self, cols_str):
        cols_list = cols_str.split(',')
        return ','.join( [":%s" % str(x+1) for x in range(len(cols_list))] )

    # 猜测表的时间字段名称
    def _search_table_date_field(self, cols_str):
        # 维度表没有时间最短
        if not self.sdate and not self.edate:
            return None

        for field in conf.date_field_liset:
            if cols_str.count(field) > 0:
                return field

        raise Exception("table: %s date field not in the enum." % self.table_name)

    # 用于处理一下pg和oracle字段名不一样的情况
    def _get_ora_table_cols_str(self, pg_cols_str):
        return conf.special_ora_fields.get(self.ora_table_name, pg_cols_str)


    def _get_sql_dict(self):
        pg_cols_str = self._get_pg_table_cols_str()

        ora_cols_str = self._get_ora_table_cols_str(pg_cols_str)

        sql_dict = {'fields':pg_cols_str,
                    'ora_fields':ora_cols_str,
                    'table_name':self.table_name,
                    'ora_table_name':self.ora_table_name,
                    'date_field':self._search_table_date_field(pg_cols_str),
                    'sdate':self.sdate,
                    'edate':self.edate}
        return sql_dict

    def _get_pg_select_sql(self):
        # 维度表不传入日期
        if not self.sdate and not self.edate:
            sql = """select %(fields)s from %(table_name)s """
        else:
            sql = """select %(fields)s from %(table_name)s where %(date_field)s >= Date'%(sdate)s' and %(date_field)s < Date'%(edate)s' + 1"""

        return sql

    def _normal_get_pg_table_datas(self):
        sql = self._get_pg_select_sql()

        datas, _ = self.dbmanage.query_pg_datas(sql % self.sql_dict)

        if not datas:
            logging.warning("oracle table:%(ora_table_name)s [%(sdate)s ~ %(edate)s] don't have data." % self.sql_dict )

        return datas

    def _iter_get_pg_table_datas(self):
        sql = self._get_pg_select_sql()

        return self.dbmanage.pg_iter(sql % self.sql_dict)


    def _delete_ora_datas(self):
        # 维度表删除
        if not self.sdate and not self.edate:
            sql = """ delete from %(ora_table_name)s """
        else:
            sql = """ delete from %(ora_table_name)s where %(date_field)s >= Date'%(sdate)s' and %(date_field)s < Date'%(edate)s' + 1 """

        self.dbmanage.ora_execute(sql % self.sql_dict)


    def _pg_data_to_ora(self, ora_datas):
        self.sql_dict['values_str'] = self._get_insert_values_str( self.sql_dict.get('ora_fields') )
        sql = """ insert into %(ora_table_name)s ( %(ora_fields)s ) values (%(values_str)s) """ % self.sql_dict
        self.dbmanage.ora_executemany(sql, ora_datas)

    # overwrite Greenlet
    def _run(self):
        start = datetime.datetime.now()
        print "table: %s running..." % self.table_name

        try:
            self.sql_dict = self._get_sql_dict()
        except Exception, e:
            logging.error( str(e))
            return

        self._delete_ora_datas()
        pg_datas = []
        count = 0
        for row in self._iter_get_pg_table_datas():
            pg_datas.append( row )
            count += 1

            if count > conf.ITER_BUF_SIZE:
                self._pg_data_to_ora(pg_datas)
                pg_datas = []
                count = 0

        if pg_datas:
            self._pg_data_to_ora(pg_datas)

        end = datetime.datetime.now()

        print "table: %s cost time: %s " % (self.table_name, str(end-start))


    def _normal_run(self):
        start = datetime.datetime.now()

        self.sql_dict = self._get_sql_dict()

        ora_datas = self._normal_get_pg_table_datas()
        self._delete_ora_datas()
        self._pg_data_to_ora(ora_datas)

        end = datetime.datetime.now()

        print "table: %s cost time: %s " % (self.table_name, str(end-start))
