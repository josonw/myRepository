#-*- coding: UTF-8 -*-

import os
import sys
import calendar
import datetime
import time


class PublicFunc():
    """ 公共函数类 """
    def __init__(self, debug = 0):
        """ 初始化类的参数 """


    def __del__(self):
        """ 做一些清理操作 """


    def write_log(self, info):
        """ 信息打印函数 """
        print """[ %s ] %s""" % (time.strftime('%F %X',time.localtime()), info)


    def datetime_offset_by_month(self, datetime1, n = 1):
        # create a shortcut object for one day
        one_day = datetime.timedelta(days = 1)
        q,r = divmod(datetime1.month + n, 12)
        datetime2 = datetime.datetime(
            datetime1.year + q, r + 1, 1) - one_day
        if datetime1.month != (datetime1 + one_day).month:
            return datetime2
        if datetime1.day >= datetime2.day:
            return datetime2
        return datetime2.replace(day = datetime1.day)


    def trunc(self, date_str, mode="MM"):
        """ 在输入日期的基础上添加多少天（可以是负值）后返回（默认返回格式为yyyy-mm-dd） """
        try:
            datetime.datetime.strptime(date_str, '%Y-%m-%d')
        except Exception, e:
            self.write_log("the format of stat date(" + date_str + ") is error, must yyyy-mm-dd")
            return 1
        result = ''
        base_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        mode = mode.upper()

        if mode == "MM":   # 获取当前月份的第一天
            result = base_date.strftime("%Y-%m") + '-01'
        elif mode == "Q":  # 获取当前季度的第一天
            year = base_date.strftime("%Y")
            month = base_date.strftime("%m")
            month_dict =  { '01':'01',
                            '02':'01',
                            '03':'01',
                            '04':'04',
                            '05':'04',
                            '06':'04',
                            '07':'07',
                            '08':'07',
                            '09':'07',
                            '10':'10',
                            '11':'10',
                            '12':'10' }
            result = year + '-' + month_dict.get(month) + '-' + '01'
        elif mode == "YYYY": # 获取一年第一天
            result = base_date.strftime("%Y") + '-01-01'
        elif mode == "IW":   # 获取当前周的第一天
            days_ahead = 0 - base_date.weekday()
            result_date = base_date + datetime.timedelta(days_ahead)
            result = result_date.strftime("%Y-%m-%d")
        elif mode == "NM":   # 获取下月第一天
            year = int( base_date.strftime("%Y") )
            month = int( base_date.strftime("%m") ) + 1
            if month>12:
                year += 1
                month = '01'
            elif month<10:
                month = '0%s' % month
            else:
                month = '%s' % month
            result = '%s-%s-01' % (year, month)
        else :
            result = base_date.strftime("%Y-%m-%d")
        return result


    def last_day(self, date_str):
        """ date_str 的当月最后一天"""
        base_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        d = calendar.monthrange(base_date.year, base_date.month)
        return "%s-%s" % (base_date.strftime("%Y-%m"), d[1])


    def add_days(self, date_str, days_num, fmt = 'yyyy-mm-dd'):
        """ 在输入日期的基础上添加多少天（可以是负值）后返回（默认返回格式为yyyy-mm-dd） """
        try:
            datetime.datetime.strptime(date_str, '%Y-%m-%d')
        except Exception, e:
            self.write_log("the format of stat date(" + date_str + ") is error, must yyyy-mm-dd")
            return 1
        base_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        return (base_date + datetime.timedelta(days=days_num)).strftime("%Y-%m-%d")


    def add_months(self, date_str, months_num):
        """ 在输入日期前添加多少天后返回（默认返回格式为yyyy-mm-ddd） """
        try:
            datetime.datetime.strptime(date_str, '%Y-%m-%d')
        except Exception, e:
            self.write_log("the format of stat date(" + date_str + ") is error, must yyyy-mm-dd")
            return 1

        base_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        edate = self.datetime_offset_by_month(base_date, int(months_num))
        return edate.strftime('%Y-%m-%d')

    def date_define(self, date_str):
        """
        输入：日期 YYYY-MM-DD
        输出：日期字典,1,2,3,4,5,6,7,14,30,自然周,自然月等
        """
        begin_day = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        end_day = begin_day + datetime.timedelta(days=1)

        ld_daybegin = begin_day.strftime('%Y-%m-%d')
        ld_dayend = end_day.strftime('%Y-%m-%d')

        num_begin = ld_daybegin.replace('-', '')

        ld_1daylater = (begin_day + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        ld_2daylater = (begin_day + datetime.timedelta(days=2)).strftime('%Y-%m-%d')

        ld_1dayago = (begin_day - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        ld_2dayago = (begin_day - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
        ld_3dayago = (begin_day - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
        ld_4dayago = (begin_day - datetime.timedelta(days=4)).strftime('%Y-%m-%d')
        ld_5dayago = (begin_day - datetime.timedelta(days=5)).strftime('%Y-%m-%d')
        ld_6dayago = (begin_day - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
        ld_7dayago = (begin_day - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        ld_8dayago = (begin_day - datetime.timedelta(days=8)).strftime('%Y-%m-%d')
        ld_13dayago = (begin_day - datetime.timedelta(days=13)).strftime('%Y-%m-%d')
        ld_14dayago = (begin_day - datetime.timedelta(days=14)).strftime('%Y-%m-%d')
        ld_15dayago = (begin_day - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
        ld_21dayago = (begin_day - datetime.timedelta(days=21)).strftime('%Y-%m-%d')
        ld_27dayago = (begin_day - datetime.timedelta(days=27)).strftime('%Y-%m-%d')
        ld_28dayago = (begin_day - datetime.timedelta(days=28)).strftime('%Y-%m-%d')
        ld_29dayago = (begin_day - datetime.timedelta(days=29)).strftime('%Y-%m-%d')
        ld_30dayago = (begin_day - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
        ld_31dayago = (begin_day - datetime.timedelta(days=31)).strftime('%Y-%m-%d')
        ld_32dayago = (begin_day - datetime.timedelta(days=32)).strftime('%Y-%m-%d')
        ld_59dayago = (begin_day - datetime.timedelta(days=59)).strftime('%Y-%m-%d')
        ld_60dayago = (begin_day - datetime.timedelta(days=60)).strftime('%Y-%m-%d')
        ld_89dayago = (begin_day - datetime.timedelta(days=89)).strftime('%Y-%m-%d')
        ld_90dayago = (begin_day - datetime.timedelta(days=90)).strftime('%Y-%m-%d')
        ld_364dayago = (begin_day - datetime.timedelta(days=364)).strftime('%Y-%m-%d')
        ld_365dayago = (begin_day - datetime.timedelta(days=365)).strftime('%Y-%m-%d')

        weekbegin = begin_day - datetime.timedelta(days=begin_day.weekday())
        weekend = begin_day + datetime.timedelta(days=6-begin_day.weekday()) + datetime.timedelta(days=1)
        # ld_weekbegin 本周第一天
        # ld_weekend 本周最后一天的下一天
        ld_weekbegin = (weekbegin).strftime('%Y-%m-%d')
        ld_weekend = (weekend).strftime('%Y-%m-%d')
        # ld_1weekago_begin 上周第一天
        # ld_1weekago_end 上周最后一天的下一天
        ld_1weekago_begin = (weekbegin - datetime.timedelta(days=7*1)).strftime('%Y-%m-%d')
        ld_1weekago_end = (weekend - datetime.timedelta(days=7*1)).strftime('%Y-%m-%d')
        ld_2weekago_begin = (weekbegin - datetime.timedelta(days=7*2)).strftime('%Y-%m-%d')
        ld_2weekago_end = (weekend - datetime.timedelta(days=7*2)).strftime('%Y-%m-%d')
        ld_3weekago_begin = (weekbegin - datetime.timedelta(days=7*3)).strftime('%Y-%m-%d')
        ld_3weekago_end = (weekend - datetime.timedelta(days=7*3)).strftime('%Y-%m-%d')
        ld_4weekago_begin = (weekbegin - datetime.timedelta(days=7*4)).strftime('%Y-%m-%d')
        ld_4weekago_end = (weekend - datetime.timedelta(days=7*4)).strftime('%Y-%m-%d')
        ld_5weekago_begin = (weekbegin - datetime.timedelta(days=7*5)).strftime('%Y-%m-%d')
        ld_5weekago_end = (weekend - datetime.timedelta(days=7*5)).strftime('%Y-%m-%d')
        ld_6weekago_begin = (weekbegin - datetime.timedelta(days=7*6)).strftime('%Y-%m-%d')
        ld_6weekago_end = (weekend - datetime.timedelta(days=7*6)).strftime('%Y-%m-%d')
        ld_7weekago_begin = (weekbegin - datetime.timedelta(days=7*7)).strftime('%Y-%m-%d')
        ld_7weekago_end = (weekend - datetime.timedelta(days=7*7)).strftime('%Y-%m-%d')
        ld_8weekago_begin = (weekbegin - datetime.timedelta(days=7*8)).strftime('%Y-%m-%d')
        ld_8weekago_end = (weekend - datetime.timedelta(days=7*8)).strftime('%Y-%m-%d')

        # ld_monthbegin 本月第一天（自然月）
        # ld_monthend 下月第一天
        ld_monthbegin = datetime.date(begin_day.year, begin_day.month, 1).strftime('%Y-%m-%d')
        ld_1monthago_end = ld_monthbegin
        if begin_day.month == 12:
            ld_monthend = datetime.date(begin_day.year+1, 1, 1).strftime('%Y-%m-%d')
        else:
            ld_monthend = datetime.date(begin_day.year, begin_day.month+1, 1).strftime('%Y-%m-%d')
        # ld_1monthago_begin 上个月第一天
        # 1d_1monthago_end 本月第一天
        month = begin_day.month - 1
        year = begin_day.year
        if month <= 0:
            month += 12
            year -= 1
        ld_1monthago_begin = datetime.date(year, month, 1).strftime('%Y-%m-%d')
        ld_2monthago_end = ld_1monthago_begin
        # ld_2monthago_begin 本月前第二个月第一天
        # 1d_2monthago_end 本月前第一个月第一天
        month = begin_day.month - 2
        year = begin_day.year
        if month <= 0:
            month += 12
            year -= 1
        ld_2monthago_begin = datetime.date(year, month, 1).strftime('%Y-%m-%d')
        ld_3monthago_end = ld_2monthago_begin
        # ld_3monthago_begin 本月前第三个月第一天
        # 1d_3monthago_end 本月前第二个月第一天
        month = begin_day.month - 3
        year = begin_day.year
        if month <= 0:
            month += 12
            year -= 1
        ld_3monthago_begin = datetime.date(year, month, 1).strftime('%Y-%m-%d')

        # 确定扫描数据(1,2,3,4,5,6,7,14,30,自然周,自然月)的起始日期
        if ld_29dayago > ld_monthbegin:
            ld_begin = ld_monthbegin
        else:
            ld_begin = ld_29dayago

        # 确定扫描数据(1,2,3,4,5,6,7,14,30,自然周,自然月)的终止日期
        if ld_weekend > ld_monthend:
            ld_end = ld_weekend
        else:
            ld_end = ld_monthend

        # top的扫描数据起始日期
        if ld_29dayago < ld_1monthago_begin:
            ld_top100_begin = ld_29dayago
        else:
            ld_top100_begin = ld_1monthago_begin

        #
        def date_to_unix(date_str):
            return int(time.mktime( time.strptime(date_str, "%Y-%m-%d") ))

        ld_daybegin_unix = date_to_unix(ld_daybegin)
        ld_dayend_unix = date_to_unix(ld_dayend)


        date_dic = {
            "ld_daybegin": ld_daybegin,
            "ld_dayend": ld_dayend,
            "ld_begin": ld_begin,
            "ld_end": ld_end,
            'num_begin': num_begin,
            "ld_top100_begin": ld_top100_begin,

            'ld_1daylater': ld_1daylater,
            'ld_2daylater': ld_2daylater,

            "ld_1dayago": ld_1dayago,
            "ld_2dayago": ld_2dayago,
            "ld_3dayago": ld_3dayago,
            "ld_4dayago": ld_4dayago,
            "ld_5dayago": ld_5dayago,
            "ld_6dayago": ld_6dayago,
            "ld_7dayago": ld_7dayago,
            "ld_8dayago": ld_8dayago,
            "ld_13dayago": ld_13dayago,
            "ld_14dayago": ld_14dayago,
            "ld_15dayago": ld_15dayago,
            "ld_21dayago": ld_21dayago,
            "ld_27dayago": ld_27dayago,
            "ld_28dayago": ld_28dayago,
            "ld_29dayago": ld_29dayago,
            "ld_30dayago": ld_30dayago,
            "ld_31dayago": ld_31dayago,
            "ld_32dayago": ld_32dayago,
            "ld_59dayago": ld_59dayago,
            'ld_60dayago': ld_60dayago,
            "ld_89dayago": ld_89dayago,
            "ld_90dayago": ld_90dayago,
            "ld_364dayago": ld_364dayago,
            "ld_365dayago": ld_365dayago,

            "ld_weekbegin": ld_weekbegin,
            "ld_weekend": ld_weekend,
            "ld_monthbegin": ld_monthbegin,
            "ld_monthend": ld_monthend,

            "ld_1weekago_begin": ld_1weekago_begin,
            "ld_1weekago_end": ld_1weekago_end,
            "ld_2weekago_begin": ld_2weekago_begin,
            "ld_2weekago_end": ld_2weekago_end,
            "ld_3weekago_begin": ld_3weekago_begin,
            "ld_3weekago_end": ld_3weekago_end,
            "ld_4weekago_begin": ld_4weekago_begin,
            "ld_4weekago_end": ld_4weekago_end,
            "ld_5weekago_begin": ld_5weekago_begin,
            "ld_5weekago_end": ld_5weekago_end,
            "ld_6weekago_begin": ld_6weekago_begin,
            "ld_6weekago_end": ld_6weekago_end,
            "ld_7weekago_begin": ld_7weekago_begin,
            "ld_7weekago_end": ld_7weekago_end,
            "ld_8weekago_begin": ld_8weekago_begin,
            "ld_8weekago_end": ld_8weekago_end,
            "ld_1monthago_begin": ld_1monthago_begin,
            "ld_1monthago_end": ld_1monthago_end,
            "ld_2monthago_begin": ld_2monthago_begin,
            "ld_2monthago_end": ld_2monthago_end,
            "ld_3monthago_begin": ld_3monthago_begin,
            "ld_3monthago_end": ld_3monthago_end,

            'ld_daybegin_unix':ld_daybegin_unix,
            'ld_dayend_unix':ld_dayend_unix,
        }
        return date_dic



PublicFunc = PublicFunc()


def main():
    print PublicFunc.last_day('2012-02-20')
    print PublicFunc.add_months('2014-10-20', 1)

if __name__ == '__main__':
    pass

