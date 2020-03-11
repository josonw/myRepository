#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_active_dim_day(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_active_dim_fct
                (
                fdate date,
                fplatformfsk bigint,
                fgamefsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fgradefsk bigint,
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

        hql = """
        use stage;

        insert overwrite table analysis.user_active_dim_fct
        partition ( dt = '%(ld_daybegin)s' )
        select '%(ld_daybegin)s' fdate,
               bpm.fplatformfsk,
               bpm.fgamefsk,
               bpm.fversionfsk,
               bpm.fterminalfsk,
               nvl(ud.fgrade,0) fgradefsk,
               count(distinct uls.fuid ) factcnt
          from stage.active_user_mid uls
          left join stage.user_attribute_dim ud
            on uls.fbpid = ud.fbpid
           and uls.fuid = ud.fuid
          join analysis.bpid_platform_game_ver_map bpm
            on bpm.fbpid = uls.fbpid
          where uls.dt = '%(ld_daybegin)s'
         group by bpm.fplatformfsk,
                  bpm.fgamefsk,
                  bpm.fversionfsk,
                  bpm.fterminalfsk,
                  nvl(ud.fgrade,0)""" % query
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
a = agg_user_active_dim_day(statDate)
a()
