#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_old_active_day(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_old_active_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                factcnt bigint,
                f7dayactcnt bigint,
                f30dayactcnt bigint,
                fweekactcnt bigint,
                fmonthactcnt bigint
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
        insert overwrite table analysis.user_old_active_fct
        partition (dt='%(ld_daybegin)s')
            select '%(ld_daybegin)s' fdate,
                 bpm.fgamefsk,
                 bpm.fplatformfsk,
                 bpm.fversionfsk,
                 bpm.fterminalfsk,
                 count(distinct if (uls.dt = '%(ld_daybegin)s', uls.fuid, null) ) factcnt,
                 count(distinct if (uls.dt >= '%(ld_6dayago)s' and uls.dt < '%(ld_dayend)s', uls.fuid, null)) f7dayactcnt,
                 count(distinct if (uls.dt >= '%(ld_29dayago)s' and uls.dt < '%(ld_dayend)s', uls.fuid, null)) f30dayactcnt,
                 count(distinct if (uls.dt >= '%(ld_weekbegin)s' and uls.dt < '%(ld_dayend)s', uls.fuid, null)) fweekactcnt,
                 count(distinct if (uls.dt >= '%(ld_monthbegin)s' and uls.dt < '%(ld_dayend)s', uls.fuid, null)) fmonthactcnt
                from (
                    select a.dt dt, a.fdate, a.fbpid, a.fuid
                      from stage.active_user_mid a
                 left join stage.user_dim b
                        on a.fbpid=b.fbpid
                       and a.fuid=b.fuid
                       and a.dt=b.dt
                       and b.dt >= '%(ld_begin)s'
                       and b.dt < '%(ld_dayend)s'
                     where b.fuid is null
                       and a.dt >= '%(ld_begin)s'
                       and a.dt < '%(ld_dayend)s'
                    ) uls
                join analysis.bpid_platform_game_ver_map bpm
                  on bpm.fbpid = uls.fbpid
                group by bpm.fgamefsk, bpm.fplatformfsk, bpm.fversionfsk, bpm.fterminalfsk """ % query
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
a = agg_user_old_active_day(statDate)
a()
