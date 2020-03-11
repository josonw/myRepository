#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_churn_user_income_data(BaseStat):
    """流失用户，历史付费金额
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_lc_loss_income_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fincome decimal(20,2),
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
        insert overwrite table analysis.user_lc_loss_income_fct
        partition ( dt = '%(ld_daybegin)s' )
          select /*+ mapjoin(d) */
           '%(ld_daybegin)s' fdate,
           d.fgamefsk,
           d.fplatformfsk,
           d.fversionfsk,
           d.fterminalfsk,
           dip,
           count(if(churn_type = 'cycle' and days = 7, a.fuid, null)) f7dayuserloss,
           count(if(churn_type = 'cycle' and days = 14, a.fuid, null)) f14dayuserloss,
           count(if(churn_type = 'cycle' and days = 30, a.fuid, null)) f30dayuserloss,
           count(if(churn_type = 'day' and days = 2, a.fuid, null)) f2eduserloss,
           count(if(churn_type = 'day' and days = 5, a.fuid, null)) f5eduserloss,
           count(if(churn_type = 'day' and days = 7, a.fuid, null)) f7eduserloss,
           count(if(churn_type = 'day' and days = 14, a.fuid, null)) f14eduserloss,
           count(if(churn_type = 'day' and days = 30, a.fuid, null)) f30eduserloss
            from stage.churn_user_mid a
            join (select b.fbpid, b.fuid, round(sum(fcoins_num * frate)) dip
                    from stage.pay_user_mid b
                    join stage.payment_stream_stg c
                      on b.fbpid = c.fbpid
                     and b.fplatform_uid = c.fplatform_uid
                     and c.fdate < '%(ld_dayend)s'
                   group by b.fbpid, b.fuid) p
              on a.fbpid = p.fbpid
             and a.fuid = p.fuid
            join analysis.bpid_platform_game_ver_map d
              on a.fbpid = d.fbpid
           group by d.fplatformfsk, d.fgamefsk, d.fversionfsk, d.fterminalfsk, dip
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
a = agg_churn_user_income_data(statDate)
a()
