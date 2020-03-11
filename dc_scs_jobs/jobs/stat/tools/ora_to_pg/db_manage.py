#!/usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os
import sys
import re
import datetime
import signal
import logging

import psycopg2
import cx_Oracle

import traceback
import threading

import conf

reload(sys)
sys.setdefaultencoding('utf-8')

#解决Oracle乱码问题
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"

#类型转换
DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None)
psycopg2.extensions.register_type(DEC2FLOAT)


class DBManage:
    # pg_dabase:master, slave
    def __init__(self, pg_dabase='master'):
        # oracle连接
        self.pg_dabase = pg_dabase

        self._db_connection()


    def __del__(self):
        pass


    def _sure_alive(self, conn):
        """确保连接正常
        """
        try:
            conn.ping()
        except Exception, e:
            print 'ping error:', e
            self._db_connection()


    def _db_connection(self):
        dsn = cx_Oracle.makedsn('210.5.191.175', '1521', 'boyaadw1')
        self.oracle_conn = cx_Oracle.connect('ANALYSIS','ANALYSIS', dsn)

        self.pg_conn     = psycopg2.connect(host=self._get_pg_dbhost('master'), database='boyaadw', user='analysis', password='analysis', port=5432)

        self.new_pg_conn = None
        if self.pg_dabase == 'slave':
            self.new_pg_conn     = psycopg2.connect(host=self._get_pg_dbhost('slave'), database='boyaadw', user='analysis', password='analysis', port=5432)


    def _get_pg_dbhost(self, pg_dabase):
        pg_db_host = None
        if pg_dabase == 'slave':
            if sys.platform == 'win32':
                pg_db_host = '175.45.5.236'
            else:
                pg_db_host = '192.168.0.90'
        elif pg_dabase == 'master':
            if sys.platform == 'win32':
                pg_db_host = '175.45.5.236'
            else:
                pg_db_host = '192.168.0.126'

        return pg_db_host


    def _get_cursor(self, conn):
        self._sure_alive(conn)
        cur = conn.cursor()
        return cur

    def _query_datas(self, conn, sql):
        cur = self._get_cursor(conn)
        datas = []
        column_names = []

        try:
            cur.execute( sql )
            column_names = [ d[0] for d in cur.description ]
            datas = cur.fetchall()
        except Exception, e:
            print sql
            raise e
        finally:
            cur.close()

        return datas, column_names

    def _execute(self, query, conn):
        cur = self._get_cursor(conn)

        t = threading.Timer(2*60, self.pg_conn.cancel)
        t.start()

        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            t.cancel()
            cur.close()

    def _executemany_rowcount(self, query, parameters, conn):
        cursor = self._get_cursor(conn)
        t = threading.Timer(2*60, conn.cancel)
        t.start()

        try:
            cursor.executemany(query, parameters)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            t.cancel()
            cursor.close()

    def _executemany_exclude_error(self, query, param_list, conn):
        param_list_len = len(param_list)

        try:
            self._executemany_rowcount(query, param_list, conn)
        except Exception as e:
            if param_list_len == 1:
                # 丢弃一条错误
                logging.error("BLIND SAVE FAILED, ERROR DESCRIBE:(%s)", str(e))
                logging.error("ERROR DATA:(%s)", str(param_list))
            else:
                half_param_len = param_list_len / 2
                self._executemany_exclude_error(query, param_list[:half_param_len], conn)
                self._executemany_exclude_error(query, param_list[half_param_len:], conn)
        except:
            print query
            print "Unexpected error:", sys.exc_info()[0]
            raise


    def _iter_qrow(self, cursor):
        for row in cursor:
            yield row
        cursor.close()


    def _iter(self, query, conn):
        """
        #迭代查询,使用yield生成器。查询返回多行，结果集数据量较大的时候较有用
        for user in db.iter("select * from user where uid >= :uid", 5):
            print user.uid, user.name
        """
        cursor = self._get_cursor(conn)
        try:
            cursor.execute(query)
        except Exception, e:
            print query
            raise e

        return self._iter_qrow(cursor)

    def pg_execute(self, query):
        self._execute(query, self.pg_conn)

    def ora_execute(self, query):
        self._execute(query, self.oracle_conn)

    def ora_iter(self, query):
        return self._iter(query, self.oracle_conn)


    def pg_iter(self, query):
        return self._iter(query, self.pg_conn)

    def query_oracle_datas(self, sql):
        return self._query_datas(self.oracle_conn, sql)


    def query_pg_datas(self, sql, db='master'):
        if db == 'slave':
            return self._query_datas(self.new_pg_conn, sql)
        else:
            return self._query_datas(self.pg_conn, sql)


    def pg_executemany(self, query, param_list):
        self._executemany_exclude_error(query, param_list, self.pg_conn)


    def ora_executemany(self, query, param_list):
        self._executemany_exclude_error(query, param_list, self.oracle_conn)
