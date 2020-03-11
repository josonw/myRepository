#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_act_user_daysgap(BaseStat):
    """活跃间隔天数
    当天活跃的用户，距离上次活跃的天数。

    user_act_days_gap_fct
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_act_days_gap_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fdaysgap bigint,
                factcnt bigint
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
        insert overwrite table analysis.user_act_days_gap_fct
        partition (dt='%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
               b.fgamefsk,
               b.fplatformfsk,
               b.fversionfsk,
               b.fterminalfsk,
               a.days,
               count(a.fuid) cnt
          from (select a.fbpid,
                       a.fuid,
                       datediff('%(ld_daybegin)s', max(b.fdate)) days
                  from stage.active_user_mid a
                  join stage.active_user_mid b
                    on a.fbpid = b.fbpid
                   and a.fuid = b.fuid
                   and b.dt < '%(ld_daybegin)s'
                 where a.dt = '%(ld_daybegin)s'
                 group by a.fbpid, a.fuid) a
          join analysis.bpid_platform_game_ver_map b
            on a.fbpid = b.fbpid
         group by b.fplatformfsk, b.fgamefsk, b.fversionfsk, b.fterminalfsk, a.days;

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
a = agg_act_user_daysgap(statDate)
a()
