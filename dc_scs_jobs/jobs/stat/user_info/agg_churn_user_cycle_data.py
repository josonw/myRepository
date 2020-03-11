#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_churn_user_cycle_data(BaseStat):
    """流失用户中间表，只存放一天
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_month_loss_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fdayuserloss bigint,
                f3dayuserloss bigint,
                f7dayuserloss bigint,
                f14dayuserloss bigint,
                f30dayuserloss bigint,
                f7daypayuserloss bigint,
                f14daypayuserloss bigint,
                f30daypayuserloss bigint
                )
                partitioned by ( dt date );

                create external table if not exists analysis.user_everyday_loss_fct
                (
                  fdate           date,
                  fplatformfsk    bigint,
                  fgamefsk        bigint,
                  fversionfsk     bigint,
                  fterminalfsk    bigint,
                  f1duserloss     bigint,
                  f7duserloss     bigint,
                  f30duseloss     bigint,
                  f14duserloss    bigint,
                  f30duserloss    bigint,
                  f7dpayuserloss  bigint,
                  f14dpayuserloss bigint,
                  f30dpayuserloss bigint,
                  f2duserloss     bigint,
                  f5duserloss     bigint,
                  f2dpayuserloss  bigint,
                  f5dpayuserloss  bigint
                )
                partitioned by ( dt date );
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
        insert overwrite table analysis.user_month_loss_fct partition
          (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 count(distinct if(a.days = 1, a.fuid, null)) fdayuserloss,
                 count(distinct if(a.days = 3, a.fuid, null)) f3dayuserloss,
                 count(distinct if(a.days = 7, a.fuid, null)) f7dayuserloss,
                 count(distinct if(a.days = 14, a.fuid, null)) f14dayuserloss,
                 count(distinct if(a.days = 30, a.fuid, null)) f30dayuserloss,
                 count(distinct if(a.days = 7, c.fuid, null)) F7DAYPAYUSERLOSS,
                 count(distinct if(a.days = 14, c.fuid, null)) F14DAYPAYUSERLOSS,
                 count(distinct if(a.days = 30, c.fuid, null)) F30DAYPAYUSERLOSS
            from stage.churn_user_mid a
            left outer join stage.pay_user_mid c
              on a.fbpid = c.fbpid
             and a.fuid = c.fuid
             and a.churn_type = 'cycle'
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.user_everyday_loss_fct
        partition (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
             b.fplatformfsk fplatformfsk, b.fgamefsk fgamefsk,
             b.fversionfsk fversionfsk, b.fterminalfsk fterminalfsk,
             0 f1duserloss,
             count(distinct if(a.days = 7, a.fuid, null)) f7duserloss,
             0 f30duseloss,
             count(distinct if(a.days = 14, a.fuid, null)) f14duserloss,
             count(distinct if(a.days = 30, a.fuid, null)) f30duserloss,
             count(distinct if(a.days = 7, c.fuid, null)) f7dpayuserloss,
             count(distinct if(a.days = 14, c.fuid, null)) f14dpayuserloss,
             count(distinct if(a.days = 30, c.fuid, null)) f30dpayuserloss,
             count(distinct if(a.days = 2, a.fuid, null)) f2duserloss,
             count(distinct if(a.days = 5, a.fuid, null)) f5duserloss,
             count(distinct if(a.days = 2, c.fuid, null)) f2dpayuserloss,
             count(distinct if(a.days = 5, c.fuid, null)) f5dpayuserloss
        from (
            select fbpid, fuid, days
            from stage.churn_user_mid
            where churn_type = 'day'
        ) a
        left outer join stage.pay_user_mid c
          on a.fbpid = c.fbpid and a.fuid = c.fuid
        join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
        group by b.fplatformfsk, b.fgamefsk, b.fversionfsk, b.fterminalfsk;
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
a = agg_churn_user_cycle_data(statDate)
a()
