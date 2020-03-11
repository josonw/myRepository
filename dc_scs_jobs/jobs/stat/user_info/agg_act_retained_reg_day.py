#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_act_retained_reg_day(BaseStat):
    """日新增用户，活跃留存
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_retained_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                f1daycnt bigint,
                f2daycnt bigint,
                f3daycnt bigint,
                f4daycnt bigint,
                f5daycnt bigint,
                f6daycnt bigint,
                f7daycnt bigint,
                f14daycnt bigint,
                f30daycnt bigint,
                f60daycnt bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create table if not exists analysis.user_back_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                f1dayregbackcnt bigint,
                f2dayregbackcnt bigint,
                f3dayregbackcnt bigint,
                f4dayregbackcnt bigint,
                f5dayregbackcnt bigint,
                f6dayregbackcnt bigint,
                f7dayregbackcnt bigint,
                f14dayregbackcnt bigint,
                f30dayregbackcnt bigint
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
        # 注意开启动态分区
        #add jar hdfs://192.168.0.92:8020/dw/udf/nexr-hive-udf-0.5.jar;
           # CREATE TEMPORARY FUNCTION trunc AS 'com.nexr.platform.hive.udf.UDFTrunc';
        res = self.hq.exe_sql("""use stage;  set hive.exec.dynamic.partition=true; """)
        if res != 0: return res

        hql = """
        drop table if exists stage.user_act_retained_reg_tmp;
        create table stage.user_act_retained_reg_tmp
        as
        select /*+ mapjoin(c)*/ a.dt fdate, c.fgamefsk,c.fplatformfsk,c.fversion_old fversionfsk,c.fterminalfsk,
        datediff('%(ld_daybegin)s', a.dt) retday,
        count(distinct a.fuid) retusernum
        from stage.user_dim a
        left semi join stage.active_user_mid b
          on a.fbpid=b.fbpid
         and a.fuid=b.fuid
         and b.dt = '%(ld_daybegin)s'
        join dim.bpid_map c
         on a.fbpid=c.fbpid
        where a.dt >= '%(ld_90dayago)s'
          and a.dt < '%(ld_dayend)s'
        group by a.dt, c.fgamefsk,c.fplatformfsk,c.fversion_old,c.fterminalfsk, datediff( '%(ld_daybegin)s', a.dt) ;


        insert overwrite table analysis.user_retained_fct
        partition( dt )
        select fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            max(f1daycnt) f1daycnt,
            max(f2daycnt) f2daycnt,
            max(f3daycnt) f3daycnt,
            max(f4daycnt) f4daycnt,
            max(f5daycnt) f5daycnt,
            max(f6daycnt) f6daycnt,
            max(f7daycnt) f7daycnt,
            max(f14daycnt) f14daycnt,
            max(f30daycnt) f30daycnt,
            max(f60daycnt) f60daycnt,
            max(f90daycnt) f90daycnt,
            fdate dt
        from (
            select fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
                if( retday=1, retusernum, 0 ) f1daycnt,
                if( retday=2, retusernum, 0 ) f2daycnt,
                if( retday=3, retusernum, 0 ) f3daycnt,
                if( retday=4, retusernum, 0 ) f4daycnt,
                if( retday=5, retusernum, 0 ) f5daycnt,
                if( retday=6, retusernum, 0 ) f6daycnt,
                if( retday=7, retusernum, 0 ) f7daycnt,
                if( retday=14, retusernum, 0 ) f14daycnt,
                if( retday=30, retusernum, 0 ) f30daycnt,
                if( retday=60, retusernum, 0 ) f60daycnt,
                if( retday=90, retusernum, 0 ) f90daycnt
            from user_act_retained_reg_tmp
            union all

            SELECT fdate,
                   fgamefsk,
                   fplatformfsk,
                   fversionfsk,
                   fterminalfsk,
                   f1daycnt,
                   f2daycnt,
                   f3daycnt,
                   f4daycnt,
                   f5daycnt,
                   f6daycnt,
                   f7daycnt,
                   f14daycnt,
                   f30daycnt,
                   f60daycnt,
                   f90daycnt
            FROM analysis.user_retained_fct
            WHERE dt >= '%(ld_90dayago)s'
              AND dt < '%(ld_dayend)s'
                    ) tmp group by fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk;

        insert overwrite table analysis.user_back_fct
        partition( dt ='%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
              fgamefsk,
              fplatformfsk,
              fversionfsk,
              fterminalfsk,
              sum(if(retday = 1, retusernum, 0)) f1dayregbackcnt,
              sum(if(retday = 2, retusernum, 0)) f2dayregbackcnt,
              sum(if(retday = 3, retusernum, 0)) f3dayregbackcnt,
              sum(if(retday = 4, retusernum, 0)) f4dayregbackcnt,
              sum(if(retday = 5, retusernum, 0)) f5dayregbackcnt,
              sum(if(retday = 6, retusernum, 0)) f6dayregbackcnt,
              sum(if(retday = 7, retusernum, 0)) f7dayregbackcnt,
              sum(if(retday = 14, retusernum, 0)) f14dayregbackcnt,
              sum(if(retday = 30, retusernum, 0)) f30dayregbackcnt
              from stage.user_act_retained_reg_tmp
              group by  fgamefsk,
                        fplatformfsk,
                        fversionfsk,
                        fterminalfsk

        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.user_retained_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        f1daycnt,
        f2daycnt,
        f3daycnt,
        f4daycnt,
        f5daycnt,
        f6daycnt,
        f7daycnt,
        f14daycnt,
        f30daycnt,
        f60daycnt,
        f90daycnt
        from analysis.user_retained_fct
        where dt >= '%(ld_90dayago)s' and dt < '%(ld_dayend)s'

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
a = agg_act_retained_reg_day(statDate)
a()
