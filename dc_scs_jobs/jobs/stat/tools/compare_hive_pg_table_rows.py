#-*- coding: UTF-8 -*-
import os
import sys
import re
import datetime

import psycopg2
import pyhs2
import commands

hive_config = {'host':'192.168.0.94', 'port':10000, 'authMechanism':'PLAIN',
                'user':'hadoop', 'password':'bydchadoop', 'database': 'analysis'}


hive_conn = pyhs2.connect(host=hive_config['host'], port=hive_config['port'], user=hive_config['user'],
                          password=hive_config['password'], database=hive_config['database'],
                          authMechanism=hive_config['authMechanism'])


pg_conn = psycopg2.connect(host='192.168.0.126', database='boyaadw', user='analysis', password='analysis', port=5432)

# 本地连接pg
# pg_conn = psycopg2.connect(host='175.45.5.236', database='boyaadw', user='analysis', password='analysis', port=5432)

def query_datas(conn, sql):
    cur = conn.cursor()
    datas = []

    try:
        cur.execute( sql )
        datas = cur.fetchall()
    except Exception, e:
        print e
    finally:
        cur.close()

    return datas

def query_one(conn, sql):
    # 建立Cursor对象
    cur = conn.cursor()

    datas = (0,)

    # 插叙数据，并获取结果
    try:
        cur.execute( sql )
        datas = cur.fetchone()
    except Exception, e:
        print e
    finally:
        cur.close()

    return datas[0]



def get_pg_table_rows(table_name, date):
    sql = """ select count(1) from  analysis.%s where fdate = DATE'%s' """ % (table_name, date)
    datas = query_one(pg_conn, sql)
    return datas

# def get_hive_table_rows(table_name, date):
#     sql = """ select count(1) from  analysis.%s where  fdate = DATE'%s' """ % (table_name, date)
#     datas = query_one(hive_conn, sql)
#     return datas

def get_hive_table_rows(table_name, date):
    out = 0
    command_str = 'hadoop fs -cat /dw/analysis/%s/dt=%s/00* | wc -l' % (table_name, date)
    lines = commands.getoutput( command_str )
    try:
        out = int(lines)
    except Exception, e:
        print e
    return out


def start_compare(table_name, date):
    pg_rows_num = get_pg_table_rows(table_name, date)
    hive_rows_num = get_hive_table_rows(table_name, date)

    print "hive_rows:" + str(hive_rows_num) + "  pg_rows:" + str(pg_rows_num) + "  differ:" + str(hive_rows_num-pg_rows_num)



if __name__ == "__main__":

    date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")

    if len(sys.argv) == 2:
        table_name = sys.argv[1]
        start_compare(table_name, date)
    elif len(sys.argv) == 3:
        table_name = sys.argv[1]
        date = sys.argv[2]
        start_compare(table_name, date)
    else:
        print "compare_hive_pg_table_rows table_name"
        print "compare_hive_pg_table_rows table_name date"
