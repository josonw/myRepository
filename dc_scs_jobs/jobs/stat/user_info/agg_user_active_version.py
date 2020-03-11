#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_active_version(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_active_version_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fentrancefsk bigint,
                fversioninfofsk varchar(50),
                factcnt bigint,
                fdevactcnt bigint
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

        insert overwrite table analysis.user_active_version_fct
        partition ( dt = '%(ld_daybegin)s' )
        select '%(ld_daybegin)s' fdate,
               bpm.fgamefsk,
               bpm.fplatformfsk,
               bpm.fversionfsk,
               bpm.fterminalfsk,
               fentrance_id fentrancefsk,
               fversion_info fversioninfofsk,
               count(distinct ls.fuid) factcnt,
               count(distinct ls.fm_imei) fdevactcnt
          from stage.active_user_mid ud
          left outer join (select fbpid,
                                  fuid,
                                  max(fentrance_id) fentrance_id,
                                  max(fversion_info) fversion_info,
                                  max(fm_imei) fm_imei
                             from user_login_stg
                            where dt = '%(ld_daybegin)s'
                            group by fbpid, fuid) ls
            on ls.fbpid = ud.fbpid
           and ls.fuid = ud.fuid
          join analysis.bpid_platform_game_ver_map bpm
            on ud.fbpid = bpm.fbpid
         where ud.dt = '%(ld_daybegin)s'
         group by bpm.fgamefsk,
                  bpm.fplatformfsk,
                  bpm.fversionfsk,
                  bpm.fterminalfsk,
                  fentrance_id,
                  fversion_info
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
a = agg_user_active_version(statDate)
a()
