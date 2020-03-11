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

class FindOraRows(Greenlet):
    def __init__(self, dbmanage, table_name, sdate=None, edate=None):
        Greenlet.__init__(self)

        self.dbmanage = dbmanage
        self.table_name = table_name
        self.sdate = sdate
        self.edate = edate

        self.sql_dict = None

    def _get_pg_table_name(self):
        special_map = {'ddz_jinbi_fcnt':'ddz_jinbi_fct'}
        return special_map.get(self.table_name, self.table_name)

    def _get_pg_table_cols_str(self):
        sql = """ select relname,attname,typname FROM pg_class c,pg_attribute a,pg_type t
                   WHERE c.relname = '%s'
                     AND c.oid = attrelid AND atttypid = t.oid AND attnum > 0 """ % self._get_pg_table_name()
        datas, _ = self.dbmanage.query_pg_datas(sql)

        if not datas:
            raise Exception("table %s is not exist." % self.table_name)

        return ','.join( [ data[1] for data in datas ] )

    # 猜测表的时间字段名称
    def _search_table_date_field(self, cols_str):
        # 维度表没有时间最短
        if not self.sdate and not self.edate:
            return None

        for field in conf.date_field_liset:
            if cols_str.count(field) > 0:
                return field

        raise Exception("table: %s date field not in the enum." % self.table_name)

    def _get_sql_dict(self):
        pg_cols_str = self._get_pg_table_cols_str()

        sql_dict = {'table_name':self.table_name,
                    'date_field':self._search_table_date_field(pg_cols_str),
                    'sdate':self.sdate,
                    'edate':self.edate}
        return sql_dict

    def _get_ora_select_sql(self):
        # 维度表不传入日期
        if not self.sdate and not self.edate:
            sql = """select count(*) from %(table_name)s"""
        else:
            sql = """select count(*) from %(table_name)s where %(date_field)s >= Date'%(sdate)s' and %(date_field)s < Date'%(edate)s' + 1 """

        return sql

    def _normal_get_ora_table_datas(self):
        datas = []

        sql = self._get_ora_select_sql()

        try:
            self.sql_dict = self._get_sql_dict()
        except Exception, e:
            logging.error( str(e))
            return datas
            # raise e

        try:
            datas, _ = self.dbmanage.query_oracle_datas(sql % self.sql_dict)
        except Exception, e:
            print self.table_name
            raise e

        rows = datas[0][0]

        if not rows:
            logging.warning("ora table:%(table_name)s [%(sdate)s ~ %(edate)s] is 0 ." % self.sql_dict )
        else:
            logging.info("ora table: %s rows: %s" % (self.table_name, str(rows) ))

        return datas


    def _run(self):
        print 'table: %s running...' % self.table_name
        self._normal_get_ora_table_datas()
        print 'table: %s finished' % self.table_name