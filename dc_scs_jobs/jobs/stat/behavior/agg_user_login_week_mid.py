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

class agg_user_login_week_mid(BaseStat):
    def create_tab(self):
        """
        user_login_week_mid：一周登录行为数据
        ffirst_login_time:   首次登录时间
        flast_login_time:    最后一次登录时间
        flogin_cnt:          登录次数
        flogin_day:          登录天数
        flogin_max_cnt:      一天内最大登录次数
        fcountry_cnt:        不同国家数,
        fprovince_cnt:       不同省份数,
        fcity_cnt:           不同城市数，过多的话说明有异常
        """

        hql = """
            create table if not exists stage.user_login_week_mid
            (
                fdate               date,
                fbpid               varchar(64),
                fuid                int,
                ffirst_login_time   string,
                flast_login_time    string,
                flogin_cnt          int,
                flogin_day          int,
                flogin_max_cnt      int,
                fcountry_cnt        int,
                fprovince_cnt       int,
                fcity_cnt           int
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
            insert overwrite table stage.user_login_week_mid
            partition(dt='%(ld_daybegin)s')
            select '%(ld_daybegin)s'        fdate,
                   fbpid,
                   fuid,
                   max(ffirst_login_time)   ffirst_login_time,
                   max(flast_login_time)    flast_login_time,
                   max(flogin_cnt)          flogin_cnt,
                   max(flogin_day)          flogin_day,
                   max(flogin_max_cnt)      flogin_max_cnt,
                   max(fcountry_cnt)        fcountry_cnt,
                   max(fprovince_cnt)       fprovince_cnt,
                   max(fcity_cnt)           fcity_cnt
              from (select fbpid,
                           fuid,
                           min(flogin_at)               ffirst_login_time,
                           max(flogin_at)               flast_login_time,
                           count(distinct flogin_at)    flogin_cnt,
                           count(distinct dt)           flogin_day,
                           null                         flogin_max_cnt,
                           count(distinct fip_country)  fcountry_cnt,
                           count(distinct fip_province) fprovince_cnt,
                           count(distinct fip_city)     fcity_cnt
                      from stage.user_login_stg
                     where dt >= '%(ld_6dayago)s'
                       and dt < '%(ld_dayend)s'
                     group by fbpid, fuid

                    union all

                    select fbpid,
                           fuid,
                           null             ffirst_login_time,
                           null             flast_login_time,
                           null             flogin_cnt,
                           null             flogin_day,
                           max(flogin_cnt)  flogin_max_cnt,
                           null             fcountry_cnt,
                           null             fprovince_cnt,
                           null             fcity_cnt
                      from (select fbpid, fuid, count(distinct flogin_at) flogin_cnt
                              from stage.user_login_stg
                             where dt >= '%(ld_6dayago)s'
                               and dt < '%(ld_dayend)s'
                             group by fbpid, fuid, dt) t1
                     group by fbpid, fuid) t2
             group by fbpid, fuid
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_login_week_mid(statDate)
    a()