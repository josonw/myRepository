#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_act_user_reg_date_data(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_reg_act_days_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                factcnt bigint,
                fdayscnt bigint
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
            insert overwrite table analysis.user_reg_act_days_fct
        partition (dt='%(ld_daybegin)s')
           select '%(ld_daybegin)s' fdate,
              c.fgamefsk,
              c.fplatformfsk,
              c.fversionfsk,
              c.fterminalfsk,
              count(distinct a.fuid) factcnt,
              case when b.fsignup_at is not null then datediff('%(ld_daybegin)s', b.dt) else 999 end fdayscnt
         from stage.active_user_mid a
         left outer join stage.user_dim b
           on a.fbpid = b.fbpid
          and a.fuid = b.fuid
          and b.dt >= date_sub('%(ld_daybegin)s',365)
          and b.dt < '%(ld_dayend)s'
         join analysis.bpid_platform_game_ver_map c
           on a.fbpid = c.fbpid
        where a.dt = '%(ld_daybegin)s'
        group by c.fplatformfsk,
                 c.fgamefsk,
                 c.fversionfsk,
                 c.fterminalfsk,
                 case when b.fsignup_at is not null then datediff('%(ld_daybegin)s', b.dt) else 999 end;
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
a = agg_act_user_reg_date_data(statDate)
a()
