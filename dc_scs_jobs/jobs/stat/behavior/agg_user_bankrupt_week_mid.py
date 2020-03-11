#-*- coding: UTF-8 -*-
# author: AnsenWen
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_bankrupt_week_mid(BaseStat):
    def create_tab(self):
        """
        建表：               一周破产救济行为数据
        frupt_cnt:           破产次数
        frupt_first_date:    首次破产时间
        frupt_last_date:     最后一次破产时间
        frupt_day:           破产的天数
        frupt_max_cnt:       一天之内破产的最大次数
        frlv_first_date:     首次救济时间
        frlv_last_date:      最后一次救济时间
        frlv_cnt:            救济次数
        frlv_day:            救济天数
        frlv_max_cnt:        一天之内救济的最大次数
        frlv_gamecoins:      救济的金币总数
        frlv_max_gamecoins:  一天之内救济的最大金币总数
        """
        hql = """
          create table if not exists stage.user_bankrupt_week_mid
          (
            fdate               date,
            fbpid               varchar(64),
            fuid                int,
            frupt_cnt           int,
            frupt_first_date    string,
            frupt_last_date     string,
            frupt_day           int,
            frupt_max_cnt       int,
            frlv_first_date     string,
            frlv_last_date      string,
            frlv_cnt            int,
            frlv_day            int,
            frlv_max_cnt        int,
            frlv_gamecoins      bigint,
            frlv_max_gamecoins  bigint
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
        insert overwrite table stage.user_bankrupt_week_mid
        partition(dt='%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
               fbpid,
               fuid,
               max(frupt_cnt)           frupt_cnt,
               min(frupt_first_date)    frupt_first_date,
               max(frupt_last_date)     frupt_last_date,
               max(frupt_day)           frupt_day,
               max(frupt_max_cnt)       frupt_max_cnt,
               min(frlv_first_date)     frlv_first_date,
               max(frlv_last_date)      frlv_last_date,
               max(frlv_cnt)            frlv_cnt,
               max(frlv_day)            frlv_day,
               max(frlv_max_cnt)        frlv_max_cnt,
               max(frlv_gamecoins)      frlv_gamecoins,
               max(frlv_max_gamecoins)  frlv_max_gamecoins
          from (select fbpid,
                       fuid,
                       count(distinct frupt_at) frupt_cnt,
                       min(frupt_at)            frupt_first_date,
                       max(frupt_at)            frupt_last_date,
                       count(distinct dt)       frupt_day,
                       null                     frupt_max_cnt,
                       null                     frlv_first_date,
                       null                     frlv_last_date,
                       null                     frlv_cnt,
                       null                     frlv_day,
                       null                     frlv_max_cnt,
                       null                     frlv_gamecoins,
                       null                     frlv_max_gamecoins
                  from stage.user_bankrupt_stg
                 where dt >= '%(ld_6dayago)s'
                   and dt < '%(ld_dayend)s'
                 group by fbpid, fuid

                union all

                select fbpid,
                       fuid,
                       null             frupt_cnt,
                       null             frupt_first_date,
                       null             frupt_last_date,
                       null             frupt_day,
                       max(frupt_cnt)   frupt_max_cnt,
                       null             frlv_first_date,
                       null             frlv_last_date,
                       null             frlv_cnt,
                       null             frlv_day,
                       null             frlv_max_cnt,
                       null             frlv_gamecoins,
                       null             frlv_max_gamecoins
                  from (select fbpid, fuid, count(distinct frupt_at) frupt_cnt
                          from stage.user_bankrupt_stg
                         where dt >= '%(ld_6dayago)s'
                           and dt < '%(ld_dayend)s'
                         group by fbpid, fuid, dt) a
                 group by fbpid, fuid

                union all

                select fbpid,
                       fuid,
                       null frupt_cnt,
                       null frupt_first_date,
                       null frupt_last_date,
                       null frupt_day,
                       null frupt_max_cnt,
                       min(flts_at) frlv_first_date,
                       max(flts_at) frlv_last_date,
                       count(distinct flts_at) frlv_cnt,
                       count(distinct dt) frlv_day,
                       null frlv_max_cnt,
                       sum(fgamecoins) frlv_gamecoins,
                       null frlv_max_gamecoins
                  from stage.user_bankrupt_relieve_stg
                 where dt >= '%(ld_6dayago)s'
                   and dt < '%(ld_dayend)s'
                 group by fbpid, fuid

                union all

                select fbpid,
                       fuid,
                       null frupt_cnt,
                       null frupt_first_date,
                       null frupt_last_date,
                       null frupt_day,
                       null frupt_max_cnt,
                       null frlv_first_date,
                       null frlv_last_date,
                       null frlv_cnt,
                       null frlv_day,
                       max(frlv_cnt) frlv_max_cnt,
                       null frlv_gamecoins,
                       max(frlv_gamecoins) frlv_max_gamecoins
                  from (select fbpid,
                               fuid,
                               count(distinct flts_at) frlv_cnt,
                               sum(fgamecoins) frlv_gamecoins
                          from stage.user_bankrupt_relieve_stg
                         where dt >= '%(ld_6dayago)s'
                           and dt < '%(ld_dayend)s'
                         group by fbpid, fuid, dt) b
                group by fbpid, fuid) t
         group by fbpid, fuid
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_bankrupt_week_mid(statDate)
    a()