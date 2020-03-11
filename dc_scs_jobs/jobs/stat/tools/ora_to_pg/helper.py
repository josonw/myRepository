#!/usr/local/python272/bin/python
#-*- coding: UTF-8 -*-
import os
import sys
import re
import datetime
import time
import signal
from cStringIO import StringIO
import logging


reload(sys)
sys.setdefaultencoding('utf-8')


def read_table_list_file(file_path):
    open_file = open(file_path, 'r')

    table_name_list = []
    for line in open_file.readlines():
        table_name = line.strip('\r\n').strip('\n').strip(' ')
        if not table_name or table_name.startswith('--'):
            continue;

        table_name_list.append( table_name )

    open_file.close()
    return table_name_list


def recognize_table_type(table_name, sdate, edate):
    # -90-marketing..表示同步90天的数据
    pattern = re.compile(r'-(\d+)-(.+)', re.I)
    match = pattern.match(table_name)

    tmp_table_name, tmp_sdate, tmp_edate = table_name, sdate, edate

    sdate_s_month = get_first_day_of_month(sdate)
    edate_s_month = get_first_day_of_month(edate)

    if table_name.startswith('-dim-'):
        (tmp_table_name, tmp_sdate, tmp_edate) = (table_name.replace('-dim-',''), None, None)
    # 按月分区表
    elif table_name.startswith('-month-'):
        (tmp_table_name, tmp_sdate, tmp_edate) = (table_name.replace('-month-',''),
                                      sdate_s_month,
                                      edate_s_month)
    elif table_name.startswith('-last_month-'):
        (tmp_table_name, tmp_sdate, tmp_edate) = (table_name.replace('-last_month-',''),
                                      datetime_offset_by_month_str(sdate_s_month, -1),
                                      datetime_offset_by_month_str(edate_s_month, -1))
    elif match:
        (tmp_table_name, tmp_sdate) = (match.groups()[1], get_date_offset_by_days(sdate,-int(match.groups()[0])))

    return tmp_table_name, tmp_sdate, tmp_edate


def get_date_of_today():
    return datetime.datetime.strftime(datetime.date.today(),'%Y-%m-%d')

def get_date_offset_today(days):
    temp_date = datetime.date.today() + datetime.timedelta(days= days)
    return datetime.datetime.strftime(temp_date,'%Y-%m-%d')


def get_first_day_of_month(date_str):
    date= datetime.datetime.strptime(date_str, '%Y-%m-%d')
    return date.strftime("%Y-%m-01")

def get_date_offset_by_days(date, days):
    date_temp = datetime.datetime.strptime(date, '%Y-%m-%d')
    final_date = (date_temp + datetime.timedelta(days= days)).strftime('%Y-%m-%d')
    return final_date


# 对日期进行加或者减月份
def datetime_offset_by_month(datetime1, n = 1):
    # create a shortcut object for one day
    one_day = datetime.timedelta(days = 1)

    # first use div and mod to determine year cycle
    q,r = divmod(datetime1.month + n, 12)

    # create a datetime2
    # to be the last day of the target month
    datetime2 = datetime.datetime(
        datetime1.year + q, r + 1, 1) - one_day

    # if input date is the last day of this month
    # then the output date should also be the last
    # day of the target month, although the day
    # may be different.
    # for example:
    # datetime1 = 8.31
    # datetime2 = 9.30
    if datetime1.month != (datetime1 + one_day).month:
        return datetime2

    # if datetime1 day is bigger than last day of
    # target month, then, use datetime2
    # for example:
    # datetime1 = 10.31
    # datetime2 = 11.30
    if datetime1.day >= datetime2.day:
        return datetime2

    # then, here, we just replace datetime2's day
    # with the same of datetime1, that's ok.
    return datetime2.replace(day = datetime1.day)

# 对日期进行加或者减月份
# in, out str date
def datetime_offset_by_month_str(date, month):
    sdate = datetime.datetime.strptime(date, '%Y-%m-%d')
    edate = datetime_offset_by_month(sdate, month)
    return edate.strftime('%Y-%m-%d')


if __name__ == '__main__':
    print datetime_offset_by_month_str('2015-04-01', -1)