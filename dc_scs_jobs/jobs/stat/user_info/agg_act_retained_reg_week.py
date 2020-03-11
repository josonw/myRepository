#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_act_retained_reg_week(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_week_retained_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fweekregcnt bigint,
                f1weekcnt bigint,
                f2weekcnt bigint,
                f3weekcnt bigint,
                f4weekcnt bigint,
                f5weekcnt bigint,
                f6weekcnt bigint,
                f7weekcnt bigint,
                f8weekcnt bigint
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
        drop table if exists stage.user_register_tmp;
        create table if not exists stage.user_register_tmp
        as
        select fdate, fbpid, fuid from
        (
        select '%(ld_8weekago_begin)s' fdate, fbpid, fuid
        from stage.user_dim
        where dt>='%(ld_8weekago_begin)s' and dt < '%(ld_8weekago_end)s'
        union all
        select '%(ld_7weekago_begin)s' fdate, fbpid, fuid
        from stage.user_dim
        where dt>='%(ld_7weekago_begin)s' and dt < '%(ld_7weekago_end)s'
        union all
        select '%(ld_6weekago_begin)s' fdate, fbpid, fuid
        from stage.user_dim
        where dt>='%(ld_6weekago_begin)s' and dt < '%(ld_6weekago_end)s'
        union all
        select '%(ld_5weekago_begin)s' fdate, fbpid, fuid
        from stage.user_dim
        where dt>='%(ld_5weekago_begin)s' and dt < '%(ld_5weekago_end)s'
        union all
        select '%(ld_4weekago_begin)s' fdate, fbpid, fuid
        from stage.user_dim
        where dt>='%(ld_4weekago_begin)s' and dt < '%(ld_4weekago_end)s'
        union all
        select '%(ld_3weekago_begin)s' fdate, fbpid, fuid
        from stage.user_dim
        where dt>='%(ld_3weekago_begin)s' and dt < '%(ld_3weekago_end)s'
        union all
        select '%(ld_2weekago_begin)s' fdate, fbpid, fuid
        from stage.user_dim
        where dt>='%(ld_2weekago_begin)s' and dt < '%(ld_2weekago_end)s'
        union all
        select '%(ld_1weekago_begin)s' fdate, fbpid, fuid
        from stage.user_dim
        where dt>='%(ld_1weekago_begin)s' and dt < '%(ld_1weekago_end)s'
        union all
        select '%(ld_weekbegin)s' fdate, fbpid, fuid
        from stage.user_dim
        where dt>='%(ld_weekbegin)s' and dt < '%(ld_weekend)s'
        ) a;

        drop table if exists stage.user_retained_week_tmp;
        set hive.auto.convert.join=false;
        create table stage.user_retained_week_tmp
        as
        select a.fdate, c.fgamefsk,c.fplatformfsk,c.fversionfsk,c.fterminalfsk, datediff('%(ld_weekbegin)s', a.fdate)/7 retday,
        count(distinct a.fuid) retusernum
        from stage.user_register_tmp a
        join stage.active_user_mid b
          on a.fbpid=b.fbpid
         and a.fuid=b.fuid
         and b.dt >= '%(ld_weekbegin)s'
         and b.dt < '%(ld_weekend)s'
        join analysis.bpid_platform_game_ver_map c
         on a.fbpid=c.fbpid
        group by a.fdate, c.fgamefsk,c.fplatformfsk,c.fversionfsk,c.fterminalfsk, datediff(a.fdate, '%(ld_weekbegin)s')/7 ;


        insert overwrite table analysis.user_week_retained_fct
        partition( dt )
        select fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            max(fweekregcnt) fweekregcnt,
            max(f1weekcnt) f1weekcnt,
            max(f2weekcnt) f2weekcnt,
            max(f3weekcnt) f3weekcnt,
            max(f4weekcnt) f4weekcnt,
            max(f5weekcnt) f5weekcnt,
            max(f6weekcnt) f6weekcnt,
            max(f7weekcnt) f7weekcnt,
            max(f8weekcnt) f8weekcnt,
            fdate dt
        from (
            select fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
                0 fweekregcnt,
                if( retday=1, retusernum, 0 ) f1weekcnt,
                if( retday=2, retusernum, 0 ) f2weekcnt,
                if( retday=3, retusernum, 0 ) f3weekcnt,
                if( retday=4, retusernum, 0 ) f4weekcnt,
                if( retday=5, retusernum, 0 ) f5weekcnt,
                if( retday=6, retusernum, 0 ) f6weekcnt,
                if( retday=7, retusernum, 0 ) f7weekcnt,
                if( retday=8, retusernum, 0 ) f8weekcnt
            from user_retained_week_tmp
            union all
            select  fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            0 fweekregcnt, f1weekcnt, f2weekcnt, f3weekcnt, f4weekcnt, f5weekcnt, f6weekcnt, f7weekcnt, f8weekcnt
            from analysis.user_week_retained_fct
            where dt >= '%(ld_8weekago_begin)s' and dt < '%(ld_dayend)s'
            union all
            select a.fdate, c.fgamefsk,c.fplatformfsk,c.fversionfsk,c.fterminalfsk,
            count(distinct a.fuid) fweekregcnt,0 f1weekcnt,0 f2weekcnt,0 f3weekcnt,0 f4weekcnt,0 f5weekcnt,0 f6weekcnt,0 f7weekcnt,0 f8weekcnt
            from stage.user_register_tmp a
            join analysis.bpid_platform_game_ver_map c
             on a.fbpid=c.fbpid
            group by a.fdate, c.fgamefsk,c.fplatformfsk,c.fversionfsk,c.fterminalfsk
        ) tmp group by fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk

        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        insert overwrite table analysis.user_week_retained_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fweekregcnt,
        f1weekcnt,
        f2weekcnt,
        f3weekcnt,
        f4weekcnt,
        f5weekcnt,
        f6weekcnt,
        f7weekcnt,
        f8weekcnt
        from analysis.user_week_retained_fct
        where dt >= '%(ld_8weekago_begin)s' and dt < '%(ld_dayend)s'
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
a = agg_act_retained_reg_week(statDate)
a()
