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

class agg_user_bankrupt_day_mid(BaseStat):
    def create_tab(self):
        """
        建表：               一天破产救济行为数据
        frupt_cnt:           破产次数
        frlv_cnt:            救济次数
        frlv_gamecoins:      救济的金币总数
        """
        hql = """
          create table if not exists stage.user_bankrupt_day_mid
          (
            fdate               date,
            fbpid               varchar(64),
            fuid                int,
            frupt_cnt           int,
            frlv_cnt            int,
            frlv_gamecoins      bigint
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
        insert overwrite table stage.user_bankrupt_day_mid
        partition(dt='%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
               fbpid,
               fuid,
               max(frupt_cnt)           frupt_cnt,
               max(frlv_cnt)            frlv_cnt,
               max(frlv_gamecoins)      frlv_gamecoins
          from (select fbpid,
                       fuid,
                       count(distinct frupt_at) frupt_cnt,
                       0                        frlv_cnt,
                       0                        frlv_gamecoins
                  from stage.user_bankrupt_stg
                 where dt >= '%(ld_daybegin)s'
                   and dt < '%(ld_dayend)s'
                 group by fbpid, fuid

                 union all

                select fbpid,
                       fuid,
                       0 frupt_cnt,
                       count(distinct flts_at) frlv_cnt,
                       sum(fgamecoins) frlv_gamecoins
                  from stage.user_bankrupt_relieve_stg
                 where dt >= '%(ld_daybegin)s'
                   and dt < '%(ld_dayend)s'
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
    a = agg_user_bankrupt_day_mid(statDate)
    a()