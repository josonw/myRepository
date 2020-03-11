#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_reflux_user_play_data(BaseStat):
    """7日回流用户，玩牌用户
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_return_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fretcnt bigint,
                fretplaycnt bigint
                )
                partitioned by (dt date)
                """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

    def stat(self):
        # self.hq.debug = 0
        date = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update(date)
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """ % query)
        if res != 0: return res
        # 创建临时表
        hql ="""
        insert overwrite table analysis.user_return_fct partition
          (dt = '%(ld_daybegin)s')
          select /*+ MAPJOIN(c)*/
           '%(ld_daybegin)s' fdate,
           c.fgamefsk,
           c.fplatformfsk,
           c.fversionfsk,
           c.fterminalfsk,
           count(distinct a.fuid) fretcnt,
           count(distinct b.fuid) fretplaycnt
            from stage.reflux_user_mid a
            left outer join stage.finished_gameparty_uid_mid b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
             and b.dt = '%(ld_daybegin)s'
            join analysis.bpid_platform_game_ver_map c
              on a.fbpid = c.fbpid
            where a.dt = '%(ld_daybegin)s'
             and a.reflux = 7
             and a.reflux_type='cycle'
           group by c.fgamefsk, c.fplatformfsk, c.fversionfsk, c.fterminalfsk;

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
a = agg_reflux_user_play_data(statDate)
a()
