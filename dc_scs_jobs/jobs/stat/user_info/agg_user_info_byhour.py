#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_info_byhour(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_info_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fhourfsk bigint,
                fregcnt bigint,
                floginusercnt bigint,
                flogincnt bigint
                )
                partitioned by (dt date)
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        query = { 'statdate':self.stat_date, }
        res = self.hq.exe_sql("""set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0: return res
        # self.hq.debug = 0
        hql = """insert overwrite table analysis.user_info_fct
                partition (dt='%(statdate)s')
                select '%(statdate)s' fdate,
                       fgamefsk,
                       fplatformfsk,
                       fversionfsk,
                       fterminalfsk,
                       fhourfsk,
                       max(fregcnt) fregcnt,
                       max(floginusercnt) floginusercnt,
                       max(flogincnt) flogincnt
                  from (select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                               hour(fsignup_at) + 1 fhourfsk,
                               count(distinct fuid) fregcnt,
                               0 flogincnt,
                               0 floginusercnt
                          from stage.user_dim a
                          join analysis.bpid_platform_game_ver_map b
                            on a.fbpid = b.fbpid
                         where dt = '%(statdate)s'
                         group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, hour(fsignup_at) + 1
                        union all
                        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                               hour(flogin_at) + 1 fhourfsk,
                               0 fregcnt,
                               count(1) flogincnt,
                               count(distinct fuid) floginusercnt
                          from stage.user_login_stg a
                          join analysis.bpid_platform_game_ver_map b
                            on a.fbpid = b.fbpid
                         where dt = '%(statdate)s'
                         group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, hour(flogin_at) + 1
                         ) a
                 group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fhourfsk
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
a = agg_user_info_byhour(statDate)
a()
