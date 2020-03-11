#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_churn_user_grade_data(BaseStat):
    """流失用户，资产分布
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_lc_loss_grade_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fgrade bigint,
                f7dayuserloss bigint,
                f14dayuserloss bigint,
                f30dayuserloss bigint,
                f2eduserloss bigint,
                f5eduserloss bigint,
                f7eduserloss bigint,
                f14eduserloss bigint,
                f30eduserloss bigint
                )
                partitioned by ( dt date )
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
        hql = """
        insert overwrite table analysis.user_lc_loss_grade_fct
        partition (dt = '%(ld_daybegin)s')
        select /*+ MAPJOIN(c) */ '%(ld_daybegin)s' fdate,
                fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,  b.fgrade,
                count(distinct if(churn_type='cycle' and days=7, a.fuid, null)  ) f7dayuserloss,
                count(distinct if(churn_type='cycle' and days=14, a.fuid, null)  ) f14dayuserloss,
                count(distinct if(churn_type='cycle' and days=30, a.fuid, null)  ) f30dayuserloss,
                0 f2eduserloss,
                0 f5eduserloss,
                0 f7eduserloss,
                0 f14eduserloss,
                0 f30eduserloss
              from stage.churn_user_mid a
              join stage.user_attribute_dim b
                on a.fbpid = b.fbpid
               and a.fuid = b.fuid
              join analysis.bpid_platform_game_ver_map c
                on a.fbpid = c.fbpid
        where a.churn_type='cycle'
         group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, b.fgrade;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        insert into table analysis.user_lc_loss_grade_fct
        partition (dt = '%(ld_daybegin)s')
        select /*+ MAPJOIN(c) */ '%(ld_daybegin)s' fdate,
                fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,  b.fgrade,
                0 f7dayuserloss,
                0 f14dayuserloss,
                0 f30dayuserloss,
                count(distinct if(churn_type='day' and days=2, a.fuid, null)  ) f2eduserloss,
                count(distinct if(churn_type='day' and days=5, a.fuid, null)  ) f5eduserloss,
                count(distinct if(churn_type='day' and days=7, a.fuid, null)  ) f7eduserloss,
                count(distinct if(churn_type='day' and days=14, a.fuid, null)  ) f14eduserloss,
                count(distinct if(churn_type='day' and days=30, a.fuid, null)  ) f30eduserloss
              from stage.churn_user_mid a
              join stage.user_attribute_dim b
                on a.fbpid = b.fbpid
               and a.fuid = b.fuid
              join analysis.bpid_platform_game_ver_map c
                on a.fbpid = c.fbpid
        where a.churn_type='day'
         group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, b.fgrade;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        insert overwrite table analysis.user_lc_loss_grade_fct
        partition (dt = '%(ld_daybegin)s')
        select  fdate,
                fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,  fgrade,
                max(f7dayuserloss) f7dayuserloss,
                max(f14dayuserloss) f14dayuserloss,
                max(f30dayuserloss) f30dayuserloss,
                max(f2eduserloss) f2eduserloss,
                max(f5eduserloss) f5eduserloss,
                max(f7eduserloss) f7eduserloss,
                max(f14eduserloss) f14eduserloss,
                max(f30eduserloss) f30eduserloss
              from  analysis.user_lc_loss_grade_fct a
             where dt = '%(ld_daybegin)s'
        group by fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fgrade;
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
a = agg_churn_user_grade_data(statDate)
a()

