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

class agg_user_bankrupt_relieve_all(BaseStat):
    def create_tab(self):
        """
        用户破产救济全量表
        ffirst_relieve_time：首次救济时间
        flast_relieve_time： 最后一次救济时间
        frelieve_cnt：       救济次数
        frelieve_day：       救济天数
        fgamecoins：         救济金币数
        """
        hql = """
          create table if not exists stage.user_bankrupt_relieve_all
          (
            fdate                 date,
            fbpid                 varchar(64),
            fuid                  int,
            ffirst_relieve_time   string,
            flast_relieve_time    string,
            frelieve_cnt          int,
            frelieve_day          int,
            fgamecoins            bigint
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
          insert overwrite table stage.user_bankrupt_relieve_all
          partition(dt='%(ld_daybegin)s')
          select '%(ld_daybegin)s'          fdate,
                 fbpid,
                 fuid,
                 min(ffirst_relieve_time)   ffirst_relieve_time,
                 max(flast_relieve_time)    flast_relieve_time,
                 sum(frelieve_cnt)          frelieve_cnt,
                 sum(frelieve_day)          frelieve_day,
                 sum(fgamecoins)            fgamecoins
           from
           (
                select fbpid,
                       fuid,
                       ffirst_relieve_time,
                       flast_relieve_time,
                       frelieve_cnt,
                       frelieve_day,
                       fgamecoins
                from stage.user_bankrupt_relieve_all
                where dt = '%(ld_1dayago)s'

                union all

                select fbpid,
                       fuid,
                       min(flts_at)         ffirst_relieve_time,
                       min(flts_at)         flast_relieve_time,
                       count(1)             frelieve_cnt,
                       1                    frelieve_day,
                       sum(fgamecoins)      fgamecoins
                  from stage.user_bankrupt_relieve_stg
                 where dt = '%(ld_daybegin)s'
                 group by fbpid, fuid
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
    a = agg_user_bankrupt_relieve_all(statDate)
    a()
