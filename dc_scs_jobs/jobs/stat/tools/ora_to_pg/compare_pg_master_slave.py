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

class ComparePGMasterSlave(Greenlet):
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


    # 猜测表的时间字段名称
    def _search_table_date_field(self, cols_str):
        # 维度表没有时间最短
        if not self.sdate and not self.edate:
            return None

        for field in conf.date_field_liset:
            if cols_str.count(field) > 0:
                return field

        raise Exception("table: %s date field not in the enum." % self.table_name)


    def _get_table_rows_sql(self):

        if not self.sdate and not self.edate:
            sql = """ select count(*) rows_num from %(table_name)s """ % self.sql_dict
        else:
            sql = """select count(*) rows_num from %(table_name)s
            where %(date_field)s >= Date'%(sdate)s' and %(date_field)s < Date'%(edate)s' + interval'1 day' """ % self.sql_dict
        return sql



    def _get_old_db_rows(self, sql):
        datas, _ = self.dbmanage.query_pg_datas(sql)
        if datas:
            return datas[0][0]
        else:
            return 0

    def _get_new_db_rows(self, sql):
        datas, _ = self.dbmanage.query_pg_datas(sql, db='slave')
        if datas:
            return datas[0][0]
        else:
            return 0

    def _real_compare(self):
        sql = self._get_table_rows_sql()

        old_data = self._get_old_db_rows(sql)
        new_data = self._get_new_db_rows(sql)

        logging.info("table:%s, old_data:%s, new_data:%s, old-new:%s "
                      %(self.table_name, str(old_data), str(new_data), str(old_data - new_data )) )


    def _get_sql_dict(self):
        pg_cols_str = self._get_pg_table_cols_str()

        # ora_cols_str = self._get_ora_table_cols_str(pg_cols_str)

        sql_dict = {'table_name':self.table_name,
                    'ora_table_name':self.ora_table_name,
                    'date_field':self._search_table_date_field(pg_cols_str),
                    'sdate':self.sdate,
                    'edate':self.edate}
        return sql_dict


    # overwrite Greenlet
    def _run(self):
        start = datetime.datetime.now()
        print "table: %s running..." % self.table_name

        try:
            self.sql_dict = self._get_sql_dict()
        except Exception, e:
            logging.error( str(e))
            return

        self._real_compare()

        end = datetime.datetime.now()

        print "table: %s cost time: %s " % (self.table_name, str(end-start))

