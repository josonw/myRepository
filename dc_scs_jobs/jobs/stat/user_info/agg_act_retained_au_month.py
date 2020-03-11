#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_act_retained_au_month(BaseStat):
    """自然月活跃用户留存
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_month_retained_au_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fmonthaucnt bigint,
                f1monthcnt bigint,
                f2monthcnt bigint,
                f3monthcnt bigint
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
        res = self.hq.exe_sql("""use stage;
            set hive.exec.dynamic.partition=true;
            """)
        if res != 0: return res

        hql = """
        drop table if exists stage.user_au_month_tmp;
        create table if not exists stage.user_au_month_tmp
        as
        select 
        distinct
        case when dt>='%(ld_3monthago_begin)s' and dt < '%(ld_3monthago_end)s' then '%(ld_3monthago_begin)s' 
             when dt>='%(ld_2monthago_begin)s' and dt < '%(ld_2monthago_end)s' then '%(ld_2monthago_begin)s' 
             when dt>='%(ld_1monthago_begin)s' and dt < '%(ld_1monthago_end)s' then '%(ld_1monthago_begin)s' 
             when dt>='%(ld_monthbegin)s' and dt < '%(ld_monthend)s' then '%(ld_monthbegin)s' 
        end fdate, fbpid, fuid 
        from stage.active_user_mid
        where dt>='%(ld_3monthago_begin)s' and dt < '%(ld_monthend)s';


        drop table if exists stage.user_act_retained_au_month_tmp;
        create table stage.user_act_retained_au_month_tmp
        as
        select a.fdate, c.fgamefsk,c.fplatformfsk,c.fversionfsk,c.fterminalfsk, round(datediff('%(ld_monthbegin)s',a.fdate)/30) retday,
        count(distinct a.fuid) retusernum
        from stage.user_au_month_tmp a
        join stage.active_user_mid b
          on a.fbpid=b.fbpid
         and a.fuid=b.fuid
         and b.dt >= '%(ld_monthbegin)s'
         and b.dt < '%(ld_monthend)s'
        join analysis.bpid_platform_game_ver_map c
         on a.fbpid=c.fbpid
        group by a.fdate, c.fgamefsk,c.fplatformfsk,c.fversionfsk,c.fterminalfsk, round(datediff('%(ld_monthbegin)s', a.fdate)/30);


        insert overwrite table analysis.user_month_retained_au_fct
        partition( dt )
        select fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            max(fmonthaucnt) fmonthaucnt,
            max(f1monthcnt) f1monthcnt,
            max(f2monthcnt) f2monthcnt,
            max(f3monthcnt) f3monthcnt,
            fdate dt
        from (
            select fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
                0 fmonthaucnt,
                if( retday=1, retusernum, 0 ) f1monthcnt,
                if( retday=2, retusernum, 0 ) f2monthcnt,
                if( retday=3, retusernum, 0 ) f3monthcnt
            from stage.user_act_retained_au_month_tmp
            union all
            select  fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            0 fmonthaucnt, f1monthcnt, f2monthcnt, f3monthcnt
            from analysis.user_month_retained_au_fct
            where dt >= '%(ld_3monthago_begin)s' and dt < '%(ld_dayend)s'
            union all
            select a.fdate, c.fgamefsk,c.fplatformfsk,c.fversionfsk,c.fterminalfsk,
            count(distinct a.fuid) fmonthaucnt,0 f1monthcnt,0 f2monthcnt,0 f3monthcnt
            from stage.user_au_month_tmp a
            join analysis.bpid_platform_game_ver_map c
             on a.fbpid=c.fbpid
            group by a.fdate, c.fgamefsk,c.fplatformfsk,c.fversionfsk,c.fterminalfsk
        ) tmp group by fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk

        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.user_month_retained_au_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fmonthaucnt,
        f1monthcnt,
        f2monthcnt,
        f3monthcnt
        from analysis.user_month_retained_au_fct
        where dt >= '%(ld_3monthago_begin)s' and dt < '%(ld_dayend)s'
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
a = agg_act_retained_au_month(statDate)
a()
