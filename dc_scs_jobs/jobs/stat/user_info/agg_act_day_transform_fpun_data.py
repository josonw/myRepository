#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_act_day_transform_fpun_data(BaseStat):
    """付费场景，活跃N天后首次付费情况。
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_active_first_pay_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                faday_cnt bigint,
                fuser_cnt bigint,
                fpay_cnt decimal(20,2)
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
        insert overwrite table analysis.user_active_first_pay_fct partition
          (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 adays faday_cnt,
                 count(distinct fuid) fuser_cnt,
                 dip fpay_cnt
            from (select /*+ MAPJOIN(m) */ m.fplatformfsk,
                         m.fgamefsk,
                         m.fversionfsk,
                         m.fterminalfsk,
                         a.fuid,
                         a.ffirst_pay_income dip,
                         count(distinct
                               if( b.fdate='%(ld_daybegin)s', null, b.fdate )) adays
                    from stage.pay_user_mid a
                    join stage.active_user_mid b
                      on a.fbpid = b.fbpid
                     and a.fuid = b.fuid
                     and b.dt >= '%(ld_59dayago)s'
                     and b.dt < '%(ld_dayend)s'
                    join analysis.bpid_platform_game_ver_map m
                      on a.fbpid = m.fbpid
                   where a.dt = '%(ld_daybegin)s'
                   group by m.fplatformfsk,
                            m.fgamefsk,
                            m.fversionfsk,
                            m.fterminalfsk,
                            a.fbpid,
                            a.fuid,
                            a.ffirst_pay_income) tmp
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, adays, dip
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
a = agg_act_day_transform_fpun_data(statDate)
a()
