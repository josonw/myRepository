#!/usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
from gevent import monkey;monkey.patch_all()

import os
import sys
import re
import datetime
import logging

from optparse import OptionParser
import gevent
from gevent import Greenlet

import conf
import db_manage
from fct_tables_ora_to_pg import OraToPG
from find_ora_abandon_table import FindOraRows
from fct_tables_pg_to_ora import PGToOra
from compare_pg_master_slave import ComparePGMasterSlave
import helper

reload(sys)
sys.setdefaultencoding('utf-8')


def logging_seting(options):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename= ( "./%s.log" % options.module),
                        filemode='w+')


def join_task(table_name_list, options, dbmanage):
    task_list = []

    if options.module == 'ora_to_pg':
        manage_class = OraToPG
    elif options.module == 'pg_to_ora':
        manage_class = PGToOra
    elif options.module == 'pg_c_m_s':
        manage_class = ComparePGMasterSlave
    else:
        manage_class = FindOraRows

    for table_name in table_name_list:
        temp_table_name, sdate, edate = helper.recognize_table_type(table_name, options.sdate, options.edate)

        task = manage_class(dbmanage, temp_table_name, sdate, edate)

        task.start()
        task_list.append(task)

    gevent.joinall( task_list )


def get_options():
    parser = OptionParser()

    parser.add_option("-f", "--file",  action="store", type="string", dest="file_name", help="table list file name")
    parser.add_option("-t", "--table", action="store", type="string", dest="table", help="table name")
    parser.add_option("-s", "--sdate", action="store", type="string", dest="sdate", help="start date eg:2015-04-27 ")
    parser.add_option("-e", "--edate", action="store", type="string", dest="edate", help="end date eg:2015-04-28 ")
    parser.add_option("-m", "--module", action="store", type="string", dest="module", default='ora_to_pg',
                        help="module ora_to_pg or find_ora or pg_to_ora or pg_c_m_s default is ora_to_pg")

    options, args = parser.parse_args()

    yesterday = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")

    if not options.file_name and not options.table:
        print 'You must input table list file name or table_name. -h for help'
        exit(0)
    elif not options.sdate:
        options.sdate = yesterday
        options.edate = yesterday
    elif not options.edate:
        options.edate = options.sdate
    return options

def start_sync(table_name_list, dbmanage, options):
    buf_table_list = []
    for table_name in table_name_list:
        buf_table_list.append( table_name )

        if len(buf_table_list) >= conf.TASK_BUF_SIZE:
            join_task( buf_table_list, options, dbmanage )
            buf_table_list = []

    join_task( buf_table_list, options, dbmanage )


def main(options):
    # 添加日志模块
    logging_seting(options)


    if options.module == 'pg_c_m_s':
        dbmanage = db_manage.DBManage(pg_dabase='new')
    else:
        dbmanage = db_manage.DBManage()

    # 单表优先
    if options.table:
        table_name_list = [options.table]
    else:
        table_name_list = helper.read_table_list_file(options.file_name)

    start_sync(table_name_list, dbmanage, options)



if __name__ == '__main__':
    start = datetime.datetime.now()

    options = get_options()
    main(options)

    end = datetime.datetime.now()

    print "total cost time: %s " % str(end-start)
