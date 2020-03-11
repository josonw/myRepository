#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_act_user_gender(BaseStat):
    """活跃用户，性别分布
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_act_gender_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fgender int,
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
        insert overwrite table analysis.user_act_gender_fct
         partition (dt = '%(ld_daybegin)s')
           select /*+ mapjoin(d)*/
                 '%(ld_daybegin)s',
                 d.fgamefsk,
                 d.fplatformfsk,
                 d.fversionfsk,
                 d.fterminalfsk,
                 nvl(b.fgender, 2) fgender,
                 count(distinct a.fuid) cnt
                  from stage.active_user_mid a
                  left outer join stage.user_dim b
                    on a.fbpid = b.fbpid
                   and a.fuid = b.fuid
                  join analysis.bpid_platform_game_ver_map d
                    on a.fbpid = d.fbpid
                 where a.dt = '%(ld_daybegin)s'
                 group by d.fplatformfsk,
                          d.fgamefsk,
                          d.fversionfsk,
                          d.fterminalfsk,
                          nvl(b.fgender, 2);
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
a = agg_act_user_gender(statDate)
a()
