#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_actdays_data(BaseStat):
    """用户，活跃天数分布
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_active_days_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fdate_range bigint,
                factdays bigint,
                fau bigint,
                fpu bigint,
                fru bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0: return res
        hql = """
        insert overwrite table analysis.user_active_days_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
             d.fgamefsk,
             d.fplatformfsk,
             d.fversionfsk,
             d.fterminalfsk,
             30,
             a.actdays,
             count(distinct a.fuid) fau,
             count(distinct b.fuid) fpu,
             count(distinct c.fuid) fru,
             count(distinct m.fuid) fbu
        from (select fbpid, fuid, count(1) actdays
                from stage.active_user_mid
               where dt >= '%(ld_29dayago)s'
                 and dt < '%(ld_dayend)s'
               group by fbpid, fuid) a
        left outer join stage.user_pay_info b
          on a.fbpid = b.fbpid
         and a.fuid = b.fuid
         and b.dt >= '%(ld_29dayago)s'
         and b.dt < '%(ld_dayend)s'
        left outer join stage.user_dim c
          on a.fbpid = c.fbpid
         and a.fuid = c.fuid
         and c.dt >= '%(ld_29dayago)s'
         and c.dt < '%(ld_dayend)s'
        left outer join (
            select distinct fbpid,fuid from stage.user_gameparty_stg
            where dt>= '%(ld_29dayago)s'
               and dt < '%(ld_dayend)s'
               and fmatch_id <> '0'
               and fmatch_id is not null
            ) m
         on a.fbpid = m.fbpid
         and a.fuid = m.fuid
        join analysis.bpid_platform_game_ver_map d
          on a.fbpid = d.fbpid
       group by d.fgamefsk,
             d.fplatformfsk,
             d.fversionfsk,
             d.fterminalfsk,
              a.actdays;

        insert into table analysis.user_active_days_fct
        partition (dt = '%(ld_daybegin)s')
        select   '%(ld_daybegin)s' fdate,
                 d.fgamefsk,
                 d.fplatformfsk,
                 d.fversionfsk,
                 d.fterminalfsk,
                 7,
                 a.actdays,
                 count(distinct a.fuid) fau,
                 count(distinct b.fuid) fpu,
                 count(distinct c.fuid) fru,
                 count(distinct m.fuid) fbu
            from (select fbpid, fuid, count(1) actdays
                    from stage.active_user_mid
                   where dt >= '%(ld_6dayago)s'
                     and dt < '%(ld_dayend)s'
                   group by fbpid, fuid) a
            left outer join stage.user_pay_info b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
             and b.dt >= '%(ld_6dayago)s'
             and b.dt < '%(ld_dayend)s'
            left outer join stage.user_dim c
              on a.fbpid = c.fbpid
             and a.fuid = c.fuid
             and c.dt >= '%(ld_6dayago)s'
             and c.dt < '%(ld_dayend)s'
            left outer join (
            select distinct fbpid,fuid from stage.user_gameparty_stg
            where dt>= '%(ld_6dayago)s'
               and dt < '%(ld_dayend)s'
               and fmatch_id <> '0'
               and fmatch_id is not null
            ) m
         on a.fbpid = m.fbpid
         and a.fuid = m.fuid
            join analysis.bpid_platform_game_ver_map d
              on a.fbpid = d.fbpid
           group by d.fgamefsk,
                 d.fplatformfsk,
                 d.fversionfsk,
                 d.fterminalfsk,
                  a.actdays;

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
a = agg_user_actdays_data(statDate)
a()
