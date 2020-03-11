#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_bs_sub_retained_data(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_bs_sub_retained_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fpname   string,
                fsubname string,
                f1daycnt bigint,
                f2daycnt bigint,
                f3daycnt bigint,
                f4daycnt bigint,
                f5daycnt bigint,
                f6daycnt bigint,
                f7daycnt bigint,
                f14daycnt bigint,
                f30daycnt bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )

        query = {'statdate':self.stat_date}

        query.update( dates_dict )

        # 注意开启动态分区
        res = self.hq.exe_sql("""use stage;
            set hive.exec.dynamic.partition=true;
            """)
        if res != 0: return res


        hql = """DROP TABLE IF EXISTS stage.user_bs_sub_retained_tmp;

                CREATE TABLE stage.user_bs_sub_retained_tmp AS
                SELECT /*+ MAPJOIN(c) */ b.dt fdate,
                     c.fgamefsk,
                     c.fplatformfsk,
                     c.fversionfsk,
                     c.fterminalfsk,
                     b.fpname,
                     b.fsubname,
                     datediff('%(ld_daybegin)s', b.dt) retday,
                     count(DISTINCT a.fuid) retusernum
                FROM stage.bs_user_mid a
                JOIN stage.bs_user_mid b
                    ON a.fbpid=b.fbpid
                    AND a.fuid=b.fuid
                    AND b.dt >= '%(ld_30dayago)s'
                    AND b.dt < '%(ld_dayend)s'
                    and a.fsubname = b.fsubname
                JOIN analysis.bpid_platform_game_ver_map c
                    ON a.fbpid=c.fbpid
                WHERE a.dt = '%(ld_daybegin)s'
                GROUP BY b.dt,
                         c.fgamefsk,
                         c.fplatformfsk,
                         c.fversionfsk,
                         c.fterminalfsk,
                         b.fpname,
                         b.fsubname,
                         datediff('%(ld_daybegin)s', b.dt);


                SET hive.exec.dynamic.partition.mode=nonstrict;


                INSERT overwrite TABLE analysis.user_bs_sub_retained_fct
                partition(dt)
                SELECT fdate,
                       fgamefsk,
                       fplatformfsk,
                       fversionfsk,
                       fterminalfsk,
                       fpname,
                       fsubname,
                       max(f1daycnt) f1daycnt,
                       max(f2daycnt) f2daycnt,
                       max(f3daycnt) f3daycnt,
                       max(f4daycnt) f4daycnt,
                       max(f5daycnt) f5daycnt,
                       max(f6daycnt) f6daycnt,
                       max(f7daycnt) f7daycnt,
                       max(f14daycnt) f14daycnt,
                       max(f30daycnt) f30daycnt,
                       fdate dt
                FROM
                  ( SELECT fdate,
                           fgamefsk,
                           fplatformfsk,
                           fversionfsk,
                           fterminalfsk,
                           fpname,
                           fsubname,
                           if(retday=1, retusernum, 0) f1daycnt,
                           if(retday=2, retusernum, 0) f2daycnt,
                           if(retday=3, retusernum, 0) f3daycnt,
                           if(retday=4, retusernum, 0) f4daycnt,
                           if(retday=5, retusernum, 0) f5daycnt,
                           if(retday=6, retusernum, 0) f6daycnt,
                           if(retday=7, retusernum, 0) f7daycnt,
                           if(retday=14, retusernum, 0) f14daycnt,
                           if(retday=30, retusernum, 0) f30daycnt
                   FROM user_bs_sub_retained_tmp
                   UNION ALL
                   SELECT fdate,
                        fgamefsk,
                        fplatformfsk,
                        fversionfsk,
                        fterminalfsk,
                        fpname,
                        fsubname,
                        if(datediff('%(ld_daybegin)s', dt)=1, 0, f1daycnt) f1daycnt,
                        if(datediff('%(ld_daybegin)s', dt)=2, 0, f2daycnt) f2daycnt,
                        if(datediff('%(ld_daybegin)s', dt)=3, 0, f3daycnt) f3daycnt,
                        if(datediff('%(ld_daybegin)s', dt)=4, 0, f4daycnt) f4daycnt,
                        if(datediff('%(ld_daybegin)s', dt)=5, 0, f5daycnt) f5daycnt,
                        if(datediff('%(ld_daybegin)s', dt)=6, 0, f6daycnt) f6daycnt,
                        if(datediff('%(ld_daybegin)s', dt)=7, 0, f7daycnt) f7daycnt,
                        if(datediff('%(ld_daybegin)s', dt)=14, 0, f14daycnt) f14daycnt,
                        if(datediff('%(ld_daybegin)s', dt)=30, 0, f30daycnt) f30daycnt
                   FROM analysis.user_bs_sub_retained_fct
                   WHERE dt >= '%(ld_30dayago)s'
                     AND dt < '%(ld_dayend)s' ) a
                GROUP BY fdate,
                         fgamefsk,
                         fplatformfsk,
                         fversionfsk,
                         fterminalfsk,
                         fpname,
                         fsubname

        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.user_bs_sub_retained_fct
        partition(dt='3000-01-01')
        SELECT fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminalfsk,
               fpname,
               fsubname,
               f1daycnt,
               f2daycnt,
               f3daycnt,
               f4daycnt,
               f5daycnt,
               f6daycnt,
               f7daycnt,
               f14daycnt,
               f30daycnt
        FROM analysis.user_bs_sub_retained_fct
        WHERE dt >= '%(ld_30dayago)s'
          AND dt < '%(ld_dayend)s'
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
a = agg_bs_sub_retained_data(statDate)
a()