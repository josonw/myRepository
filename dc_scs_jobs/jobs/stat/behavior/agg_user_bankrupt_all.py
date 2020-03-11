#-*- coding: UTF-8 -*-
# Author:AnsenWen
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_bankrupt_all(BaseStat):
    def create_tab(self):
        """
        用户破产汇总全量表
        ffirst_rupt_date：   首次破产日期
        flast_rupt_date：    最后破产日期
        fbankrupt_cnt：      破产次数
        fbankrupt_day：      破产天数
        """
        hql = """
          create table if not exists stage.user_bankrupt_all
          (
            fdate                 date,
            fbpid                 varchar(64),
            fuid                  int,
            ffirst_rupt_date      date,
            flast_rupt_date       date,
            fbankrupt_cnt         int,
            fbankrupt_day         int
          )
          partitioned by(dt date)
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
          insert overwrite table stage.user_bankrupt_all
          partition(dt='%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 fbpid,
                 fuid,
                 min(ffirst_rupt_date) ffirst_rupt_date,
                 max(flast_rupt_date) flast_rupt_date,
                 sum(fbankrupt_cnt) fbankrupt_cnt,
                 sum(fbankrupt_day) fbankrupt_day
           from
           (
                select fbpid,
                       fuid,
                       ffirst_rupt_date,
                       flast_rupt_date,
                       fbankrupt_cnt,
                       fbankrupt_day
                  from stage.user_bankrupt_all
                 where dt = '%(ld_1dayago)s'

                union all

                select fbpid,
                       fuid,
                       dt ffirst_rupt_date,
                       dt flast_rupt_date,
                       count(1) fbankrupt_cnt,
                       1 fbankrupt_day
                  from stage.user_bankrupt_stg
                 where dt = '%(ld_daybegin)s'
                 group by fbpid, fuid, dt
            ) t
            group by fbpid, fuid
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_bankrupt_all(statDate)
    a()
