#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_active_week_mid(BaseStat):
    def create_tab(self):
        """
        活跃用户7天统计中间表
        flogin_cnt：         登录次数
        flogin_day：         登录天数
        fact_day：           活跃天数
        flast_act_date：     最后活跃日期
        flast_login_date：   最后登录日期
        """
        hql = """
          create table if not exists stage.user_active_week_mid
          (
            fdate           date,
            fbpid           varchar(64),
            fuid            int,
            flogin_cnt      int,
            flogin_day      int,
            fact_day        int,
            flast_act_date  date,
            flast_login_date date
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
          insert overwrite table stage.user_active_week_mid
          partition(dt='%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 fbpid,
                 fuid,
                 coalesce(sum(fnew_login_num), 0) flogin_cnt,
                 coalesce(sum(case when fnew_login_num > 0 then 1 else 0 end), 0)  flogin_day,
                 coalesce(count(distinct dt), 0) fact_day,
                 max(dt) flast_act_date,
                 max(case when fnew_login_num > 0 then dt else null end) flast_login_date
          from stage.active_user_mid
         where dt >= '%(ld_6dayago)s'
           and dt < '%(ld_dayend)s'
         group by fbpid, fuid
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_active_week_mid(statDate)
    a()