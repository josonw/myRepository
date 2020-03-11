#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_churn_user_bankrupt_data(BaseStat):
    """流失用户，历史付费金额
    """
    def create_tab(self):
        hql = """create external table if not exists analysis.user_lc_loss_rupt_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fruptcnt bigint,
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
            insert overwrite table analysis.user_lc_loss_rupt_fct partition
            (dt = '%(ld_daybegin)s')
             select /*+ mapjoin(d) */
                   '%(ld_daybegin)s' fdate,
                   d.fgamefsk,
                   d.fplatformfsk,
                   d.fversionfsk,
                   d.fterminalfsk,
                   ruptcnt,
                   0 f7dayuserloss,
                   0 f14dayuserloss,
                   0 f30dayuserloss,
                   sum(f2eduserloss) f2eduserloss,
                   sum(f5eduserloss) f5eduserloss,
                   sum(f7eduserloss) f7eduserloss,
                   sum(f14eduserloss) f14eduserloss,
                   sum(f30eduserloss) f30eduserloss
              from (select fbpid,
                           ruptcnt,
                           count(fuid) f2eduserloss,
                           0 f5eduserloss,
                           0 f7eduserloss,
                           0 f14eduserloss,
                           0 f30eduserloss
                      from (select b.fbpid, b.fuid, count(p.fuid) ruptcnt
                              from stage.churn_user_mid b
                              left join stage.user_bankrupt_stg p
                                on b.fbpid = p.fbpid
                               and b.fuid = p.fuid
                               and p.dt = '%(ld_2dayago)s'
                             where b.churn_type = 'day'
                               and b.days = 2
                             group by b.fbpid, b.fuid
                           ) t
                     group by fbpid, ruptcnt
                    union all
                    select fbpid,
                           ruptcnt,
                           0 f2eduserloss,
                           count(fuid) f5eduserloss,
                           0 f7eduserloss,
                           0 f14eduserloss,
                           0 f30eduserloss
                      from (select b.fbpid, b.fuid, count(p.fuid) ruptcnt
                              from stage.churn_user_mid b
                              left join stage.user_bankrupt_stg p
                                on b.fbpid = p.fbpid
                               and b.fuid = p.fuid
                               and p.dt = '%(ld_5dayago)s'
                             where b.churn_type = 'day'
                               and b.days = 5
                             group by b.fbpid, b.fuid
                           ) t
                     group by fbpid, ruptcnt
                    union all
                    select fbpid,
                           ruptcnt,
                           0 f2eduserloss,
                           0 f5eduserloss,
                           count(fuid) f7eduserloss,
                           0 f14eduserloss,
                           0 f30eduserloss
                      from (select b.fbpid, b.fuid, count(p.fuid) ruptcnt
                              from stage.churn_user_mid b
                              left join stage.user_bankrupt_stg p
                                on b.fbpid = p.fbpid
                               and b.fuid = p.fuid
                               and p.dt = '%(ld_7dayago)s'
                             where b.churn_type = 'day'
                               and b.days = 7
                             group by b.fbpid, b.fuid
                           ) t
                     group by fbpid, ruptcnt
                    union all
                    select fbpid,
                           ruptcnt,
                           0 f2eduserloss,
                           0 f5eduserloss,
                           0 f7eduserloss,
                           count(fuid) f14eduserloss,
                           0 f30eduserloss
                      from (select b.fbpid, b.fuid, count(p.fuid) ruptcnt
                              from stage.churn_user_mid b
                              left join stage.user_bankrupt_stg p
                                on b.fbpid = p.fbpid
                               and b.fuid = p.fuid
                               and p.dt = '%(ld_14dayago)s'
                             where b.churn_type = 'day'
                               and b.days = 14
                             group by b.fbpid, b.fuid
                           ) t
                     group by fbpid, ruptcnt
                    union all
                    select fbpid,
                           ruptcnt,
                           0 f2eduserloss,
                           0 f5eduserloss,
                           0 f7eduserloss,
                           0 f14eduserloss,
                           count(fuid) f30eduserloss
                      from (select b.fbpid, b.fuid, count(p.fuid) ruptcnt
                              from stage.churn_user_mid b
                              left join stage.user_bankrupt_stg p
                                on b.fbpid = p.fbpid
                               and b.fuid = p.fuid
                               and p.dt = '%(ld_30dayago)s'
                             where b.churn_type = 'day'
                               and b.days = 30
                             group by b.fbpid, b.fuid
                           ) t
                     group by fbpid, ruptcnt
                  )  a
             join analysis.bpid_platform_game_ver_map d
               on a.fbpid = d.fbpid
            group by fgamefsk,
                     fplatformfsk,
                     fversionfsk,
                     fterminalfsk,
                     ruptcnt
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
a = agg_churn_user_bankrupt_data(statDate)
a()
