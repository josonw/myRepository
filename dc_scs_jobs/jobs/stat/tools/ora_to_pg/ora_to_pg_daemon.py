#!/usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
from gevent import monkey;monkey.patch_all()

import os
import sys
import re
import datetime
import time
import logging
from optparse import OptionParser

import gevent
from gevent import Greenlet

import conf
import db_manage
from fct_tables_ora_to_pg import OraToPG
from find_ora_abandon_table import FindOraRows
import helper

reload(sys)
sys.setdefaultencoding('utf-8')


def get_procedure_done_tables(dbmanage, last_run_time):
    sql = """ select to_char(max(fdate), 'yyyy-mm-dd hh24:mi:ss') table_name
                 from stage.t_common_log
                where fdate > to_date('%(last_run_time)s', 'yyyy-mm-dd hh24:mi:ss')
                  and foperation = 'end'
                  and fsubname != 'end'
               union all
               select substr(b.tab_into, 10, 1000) table_name
                 from stage.t_common_log a
                 join pkg_pro_tab_dim b
                   on upper(a.fname) = upper(b.pkg)
                  and upper(a.fsubname) = upper(b.pro)
                  and b.tab_into like 'analysis%%'
                where a.fdate > to_date('%(last_run_time)s', 'yyyy-mm-dd hh24:mi:ss')
                  and a.foperation = 'end'
                  and a.fsubname != 'end'
                group by b.tab_into """ % {'last_run_time':last_run_time}

    datas, _ = dbmanage.query_oracle_datas(sql)

    # 没有表需要同步
    if len(datas) < 2:
        return last_run_time, []

    run_time = datas[0][0]
    need_sync_table_list = datas[1:]

    return run_time, need_sync_table_list

def find_the_last_table_list(all_sync_table_list, need_sync_table_list):
    last_table_list = []
    for tmp_table_tuple in need_sync_table_list:
        for table_name in all_sync_table_list:
            if table_name.count(tmp_table_tuple[0]) > 0:
                last_table_list.append(table_name)

    return last_table_list


def join_task(table_name_list, dbmanage):
    task_list = []
    sdate = helper.get_date_offset_today(-1)
    edate = helper.get_date_offset_today(0)

    for table_name in table_name_list:
        tmp_table_name, tmp_sdate, tmp_edate = helper.recognize_table_type(table_name, sdate, edate)

        task = OraToPG(dbmanage, tmp_table_name, tmp_sdate, tmp_edate)

        task.start()
        task_list.append(task)

    gevent.joinall( task_list )

def start_sync(table_name_list, dbmanage):
    buf_table_list = []
    for table_name in table_name_list:
        buf_table_list.append( table_name )

        if len(buf_table_list) >= conf.TASK_BUF_SIZE:
            join_task( buf_table_list, dbmanage )
            buf_table_list = []

    join_task( buf_table_list, dbmanage )


def main(file_path):
    last_run_time = helper.get_date_of_today() + ' 00:00:00'

    dbmanage = db_manage.DBManage()

    all_sync_table_list = helper.read_table_list_file(file_path)

    while True:
        last_run_time, need_sync_table_list = get_procedure_done_tables(dbmanage, last_run_time)
        print 'last_run_time:', last_run_time

        table_name_list = find_the_last_table_list(all_sync_table_list, need_sync_table_list)

        print 'start_sync: ', table_name_list
        start_sync(table_name_list, dbmanage)

        time.sleep(conf.DAEMON_IDLE_SLEEP)



def get_options():
    parser = OptionParser()

    parser.add_option("-f", "--file",  action="store", type="string", dest="file_name", help="table list file name")

    options, args = parser.parse_args()

    if not options.file_name:
        print 'You must input table list file -h for help'
        exit(0)

    return options


if __name__ == '__main__':
    options = get_options()
    main(options.file_name)
