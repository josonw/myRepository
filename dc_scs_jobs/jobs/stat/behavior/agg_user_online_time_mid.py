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

class agg_user_online_time_mid(BaseStat):
    def create_tab(self):
        """
        建表：              当天在线时长数据
        flogin_first_time:  首次登录时间
        flogout_last_time:  因为数据有所缺失，最后一次登出时间，最后一次玩牌时间, 最后一次登录时间，三者取最后一个
        fonline_time        在线时长，单位秒
        """
        hql = """
          create table if not exists stage.user_online_time_mid
          (
            fdate               date,
            fbpid               varchar(64),
            fuid                int,
            flogin_first_time   string,
            flogout_last_time   string,
            fonline_time        int
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
        insert overwrite table stage.user_online_time_mid
        partition(dt='%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
               fbpid,
               fuid,
               min(flogin_first_time)   flogin_first_time,
               max(flogout_last_time)   flogout_last_time,
               unix_timestamp(max(flogout_last_time)) - unix_timestamp(min(flogin_first_time)) fonline_time
          from (select fbpid,
                       fuid,
                       min(flogin_at)   flogin_first_time,
                       null             flogout_last_time
                  from stage.user_login_stg
                 where dt = '%(ld_daybegin)s'
                   and flogin_at > '1970-01-01'
                 group by fbpid, fuid

                union all

                select fbpid,
                       fuid,
                       null             flogin_first_time,
                       max(flogout_at)  flogout_last_time
                  from stage.user_logout_stg
                 where dt = '%(ld_daybegin)s'
                   and flogout_at > '1970-01-01'
                 group by fbpid, fuid

                union all

                select fbpid,
                       fuid,
                       null             flogin_first_time,
                       max(fe_timer)    flogout_last_time
                  from stage.user_gameparty_stg
                 where dt = '%(ld_daybegin)s'
                   and fe_timer > '1970-01-01'
                 group by fbpid, fuid

                union all

                select fbpid,
                       fuid,
                       null             flogin_first_time,
                       max(flogin_at)   flogout_last_time
                  from stage.user_login_stg
                 where dt = '%(ld_daybegin)s'
                   and flogin_at > '1970-01-01'
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
    a = agg_user_online_time_mid(statDate)
    a()