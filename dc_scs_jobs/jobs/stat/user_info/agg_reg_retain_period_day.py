#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_reg_retain_period_day(BaseStat):
    """新增用户，在其后一段时间内的留存
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_reg_retain_period_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                f7daycnt bigint,
                f14daycnt bigint,
                f30daycnt bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res



    def stat(self):
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {'ld_30daylater':(datetime.datetime.strptime(self.stat_date, "%Y-%m-%d") + datetime.timedelta(days=30)).strftime('%Y-%m-%d')}
        query.update( dates_dict )
        res = self.hq.exe_sql("""use stage;  set hive.exec.dynamic.partition=true; """)
        if res != 0: return res

        hql = """
        drop table if exists stage.user_reg_retain_period_tmp;
        create table stage.user_reg_retain_period_tmp as

        SELECT /*+ mapjoin(c)*/
            a.dt fdate,
            b.dt bfdate,
            c.fgamefsk,
            c.fplatformfsk,
            c.fversionfsk,
            c.fterminalfsk,
            a.fuid,
            datediff(b.dt, a.dt) retday
        FROM stage.user_dim a
        JOIN stage.active_user_mid b
        ON a.fbpid=b.fbpid
        AND a.fuid=b.fuid
        AND b.dt >= '%(ld_30dayago)s'
        AND b.dt <= '%(ld_30daylater)s'
        JOIN analysis.bpid_platform_game_ver_map c
        ON a.fbpid=c.fbpid
        WHERE a.dt >= '%(ld_30dayago)s'
          AND a.dt < '%(ld_dayend)s'
          and a.dt != b.dt;


        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res



        hql = """
        insert overwrite table analysis.user_reg_retain_period_fct
        partition( dt )
        SELECT fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminalfsk,
               sum(f7daycnt) f7daycnt,
               sum(f14daycnt) f14daycnt,
               sum(f30daycnt) f30daycnt,
               fdate
        FROM
          ( SELECT m.fdate,
                   m.fgamefsk,
                   m.fplatformfsk,
                   m.fversionfsk,
                   m.fterminalfsk,
                   count(DISTINCT m.fuid) f7daycnt,
                   0 f14daycnt,
                   0 f30daycnt
           FROM stage.user_reg_retain_period_tmp m
           where retday>=1 AND retday<=7
           GROUP BY m.fdate,
                    m.fgamefsk,
                    m.fplatformfsk,
                    m.fversionfsk,
                    m.fterminalfsk

           UNION ALL

           SELECT m.fdate,
                   m.fgamefsk,
                   m.fplatformfsk,
                   m.fversionfsk,
                   m.fterminalfsk,
                   0 f7daycnt,
                   count(DISTINCT m.fuid) f14daycnt,
                   0 f30daycnt
           FROM stage.user_reg_retain_period_tmp m
           where retday>=1 AND retday<=14
           GROUP BY m.fdate,
                    m.fgamefsk,
                    m.fplatformfsk,
                    m.fversionfsk,
                    m.fterminalfsk

           UNION ALL

           SELECT m.fdate,
                   m.fgamefsk,
                   m.fplatformfsk,
                   m.fversionfsk,
                   m.fterminalfsk,
                   0 f7daycnt,
                   0 f14daycnt,
                   count(DISTINCT m.fuid) f30daycnt
           FROM stage.user_reg_retain_period_tmp m
           where retday>=1 AND retday<=30
           GROUP BY m.fdate,
                    m.fgamefsk,
                    m.fplatformfsk,
                    m.fversionfsk,
                    m.fterminalfsk ) foo
            group by   fdate,
                       fgamefsk,
                       fplatformfsk,
                       fversionfsk,
                       fterminalfsk
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.user_reg_retain_period_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        f7daycnt,
        f14daycnt,
        f30daycnt
        from analysis.user_reg_retain_period_fct
        where dt >= '%(ld_30dayago)s' and dt < '%(ld_dayend)s'

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
a = agg_reg_retain_period_day(statDate)
a()
