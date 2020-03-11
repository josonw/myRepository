#! /usr/local/python272/bin/python
# coding:utf-8
import os
import sys
import datetime
import time
import psycopg2

from sys import path

path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import pg_db, mysql_db

"""
此脚本用于为pg分区表预先建立分区，以供sqoop直接导入数据相应分区子表
"""

def main(dt=None):
    if not dt:
        dt = getDate(-1)
    if not is_valid_date(dt):
        print 'date is invalid !!!'
        sys.exit()
    date_partition, begin_date, end_date = getDateTuple(dt)

    conn = pg_db
    mysql = mysql_db

    sql = "select tbl_name, partition_field from sync_hive_pg where partitioned = 1"
    for row in mysql.iter(sql):
        run(conn, row['tbl_name'], row['partition_field'], date_partition, begin_date, end_date)



def run(conn, table, partition_field, date_partition, begin_date, end_date):
    conf_dic = {
        "table": table,
        "sub_table": 'child.' + table + '_' + date_partition,
        "begin_date": begin_date,
        "end_date": end_date,

        "part_field": partition_field
    }
   
    sql = """select column_name,data_type
                   from information_schema.columns
                  where table_schema||'.'||table_name='%(sub_table)s' """%conf_dic
    tableinfo = conn.query(sql)
    if not tableinfo:
        sql = """
              create table %(sub_table)s (
                check (%(part_field)s >= '%(begin_date)s'::date AND %(part_field)s < '%(end_date)s'::date)
              ) inherits (%(table)s)
              """ % conf_dic
        conn.execute(sql)

def getDate(n=0, datetype='%Y-%m-%d'):
    today = datetime.date.today()
    deltadays = datetime.timedelta(days=n)
    date = today + deltadays
    return date.strftime(datetype)


def getDateTuple(datestr):
    date = datetime.datetime.strptime(datestr, '%Y-%m-%d')

    begin_date = date
    end_date = date + datetime.timedelta(days=1)
    return (begin_date.strftime("%Y%m%d"),
            begin_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"))


def is_valid_date(datestr):
    try:
        time.strptime(datestr, "%Y-%m-%d")
        return True
    except:
        return False


if __name__ == '__main__':
    if len(sys.argv) == 2:
        dt = sys.argv[1]
        main(dt)
    else:
        main()
