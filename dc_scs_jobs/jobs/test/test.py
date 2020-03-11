#-*- coding: UTF-8 -*-
import os
import sys
from sys import path
path.append('%s/' % os.getenv('DC_SERVER_PATH'))
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStat import BasePGStat, get_stat_date
import time


class test(BasePGStat):
    """ PG执行存储过程运算 """
    def stat(self):
        sql = """
            select count(*) from dcnew.loss_user_reflux_age_dis where fdate='2017-03-16' and fgamefsk=4132314431
        """ % self.sql_dict
        self.append(sql)

        self.exe_hql_list()

if __name__ == '__main__':
    stat_date = get_stat_date()
    #生成统计实例
    t0=time.time()
    a = test(stat_date)
    t1=time.time()
    a()
    t2=time.time()
    print t1-t0,t2-t1

##END
