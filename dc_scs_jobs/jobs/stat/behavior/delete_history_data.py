#-*- coding: UTF-8 -*-
# Author: AnsenWen
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class delete_history_data(BaseStat):
    def stat(self):
        """ 重要部分，输除全量表两个月前的数据，保留最近60天的数据"""
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)

        table_list = ['user_active_all',
                        'user_bankrupt_all',
                        'user_gamecoins_all',
                        'user_gameparty_all',
                        'user_login_all',
                        'user_bankrupt_relieve_all']

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        for t in table_list:
            hql = """ alter table %s drop if exists partition(dt<='%s')
            """ % (t, query['ld_60dayago'])
            res = self.hq.exe_sql(hql)
            if res != 0: return res

        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = delete_history_data(statDate)
    a()