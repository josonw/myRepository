#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_active_paid_user_data(BaseStat):
    """建立维度表
    """
    def create_tab(self):
        hql = """create external table if not exists stage.active_paid_user_mid
                (
                fdate date,
                fbpid varchar(50),
                fuid bigint
                )
                partitioned by (dt date)
                stored as orc
                location '/dw/stage/active_paid_user_mid'"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

    def stat(self):
        # self.hq.debug = 0
        query = { 'statdate':self.stat_date }
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """ % query)
        if res != 0: return res
        hql = """
        insert overwrite table stage.active_paid_user_mid partition
          (dt = '%(statdate)s')
          select distinct '%(statdate)s' fdate, a.fbpid, a.fuid
            from stage.active_user_mid a
            join stage.pay_user_mid b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
             and b.dt < '%(statdate)s'
           where a.dt = '%(statdate)s'
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = load_active_paid_user_data(statDate)
a()
