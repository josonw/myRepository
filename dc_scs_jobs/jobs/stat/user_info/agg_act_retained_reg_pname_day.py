#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author: SkyShen
# @Date:   2015-12-02 10:10:58
# @Last Modified by:   SkyShen
# @Last Modified time: 2015-12-03 13:02:10
#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_act_retained_reg_pname_day(BaseStat):
    """日新增用户，活跃留存
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_retained_pname_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fpname varchar(100),
                total_user bigint,
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

        hql = """create table if not exists analysis.user_back_pname_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fpname  varchar(100),
                total_user bigint,
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
        drop table if exists stage.user_act_retained_reg_pname_tmp;
        create table stage.user_act_retained_reg_pname_tmp
        as
        select a.dt fdate,
        c.fgamefsk,c.fplatformfsk,c.fversionfsk,c.fterminalfsk,a.fpname,
        datediff('%(ld_daybegin)s', a.dt) retday,
        count(distinct a.fuid) total_user,
        count(distinct b.fuid) as  active_num,
        count(distinct case when b.fuid is not null and a.ffirst_play=1 then b.fuid else null end) as first_play_num
        from stage.user_gameparty_stg a
        left join stage.active_user_mid b
          on a.fbpid = b.fbpid
         and a.fuid = b.fuid
         and b.dt = '%(ld_daybegin)s'
        join analysis.bpid_platform_game_ver_map c
         on a.fbpid=c.fbpid
        where (a.dt >= '%(ld_7dayago)s' and a.dt < '%(ld_dayend)s') or a.dt='%(ld_14dayago)s' or a.dt='%(ld_30dayago)s' or a.dt='%(ld_60dayago)s'
        group by a.dt, c.fgamefsk,c.fplatformfsk,c.fversionfsk,c.fterminalfsk, a.fpname ,
        datediff( '%(ld_daybegin)s', a.dt)  ;


        insert overwrite table analysis.user_retained_pname_fct
        partition( dt )
        select fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,fpname ,
            max(total_user) total_user,
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
            fdate dt
        from (
            select fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,fpname ,
                total_user total_user,
                if( retday=1, active_num, 0 ) f1daycnt,
                if( retday=2, active_num, 0 ) f2daycnt,
                if( retday=3, active_num, 0 ) f3daycnt,
                if( retday=4, active_num, 0 ) f4daycnt,
                if( retday=5, active_num, 0 ) f5daycnt,
                if( retday=6, active_num, 0 ) f6daycnt,
                if( retday=7, active_num, 0 ) f7daycnt,
                if( retday=14, active_num, 0 ) f14daycnt,
                if( retday=30, active_num, 0 ) f30daycnt,
                if( retday=60, active_num, 0 ) f60daycnt
            from user_act_retained_reg_pname_tmp
            union all
            select  fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,fpname,
                    total_user total_user,
                    f1daycnt,f2daycnt,f3daycnt,f4daycnt,f5daycnt,f6daycnt,f7daycnt,f14daycnt,f30daycnt,f60daycnt
            from analysis.user_retained_pname_fct
            where dt >= '%(ld_60dayago)s' and dt < '%(ld_dayend)s'
        ) tmp group by fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,fpname;


        insert overwrite table analysis.user_back_pname_fct
        partition( dt )
        select fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,fpname ,
            max(total_user) total_user,
            max(f1dayregbackcnt) f1dayregbackcnt,
            max(f2dayregbackcnt) f2dayregbackcnt,
            max(f3dayregbackcnt) f3dayregbackcnt,
            max(f4dayregbackcnt) f4dayregbackcnt,
            max(f5dayregbackcnt) f5dayregbackcnt,
            max(f6dayregbackcnt) f6dayregbackcnt,
            max(f7dayregbackcnt) f7dayregbackcnt,
            max(f14dayregbackcnt) f14dayregbackcnt,
            max(f30dayregbackcnt) f30dayregbackcnt,
            fdate dt
        from (
            select fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,fpname ,
                if( retday=0, first_play_num, 0 ) total_user,
                if( retday=1, first_play_num, 0 ) f1dayregbackcnt,
                if( retday=2, first_play_num, 0 ) f2dayregbackcnt,
                if( retday=3, first_play_num, 0 ) f3dayregbackcnt,
                if( retday=4, first_play_num, 0 ) f4dayregbackcnt,
                if( retday=5, first_play_num, 0 ) f5dayregbackcnt,
                if( retday=6, first_play_num, 0 ) f6dayregbackcnt,
                if( retday=7, first_play_num, 0 ) f7dayregbackcnt,
                if( retday=14, first_play_num, 0 ) f14dayregbackcnt,
                if( retday=30, first_play_num, 0 ) f30dayregbackcnt
            from user_act_retained_reg_pname_tmp
            union all
            select  fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,fpname,
                    total_user total_user,
                    f1dayregbackcnt, f2dayregbackcnt, f3dayregbackcnt, f4dayregbackcnt, f5dayregbackcnt, f6dayregbackcnt, f7dayregbackcnt, f14dayregbackcnt, f30dayregbackcnt
            from analysis.user_back_pname_fct
            where dt >= '%(ld_30dayago)s' and dt < '%(ld_dayend)s'
        ) tmp group by fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,fpname;

        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.user_retained_pname_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fpname,
        total_user,
        f1daycnt,
        f2daycnt,
        f3daycnt,
        f4daycnt,
        f5daycnt,
        f6daycnt,
        f7daycnt,
        f14daycnt,
        f30daycnt,
        f60daycnt
        from analysis.user_retained_pname_fct
        where dt >= '%(ld_60dayago)s' and dt < '%(ld_dayend)s'

        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.user_back_pname_fct partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fpname,
        total_user,
        f1dayregbackcnt,
        f2dayregbackcnt,
        f3dayregbackcnt,
        f4dayregbackcnt,
        f5dayregbackcnt,
        f6dayregbackcnt,
        f7dayregbackcnt,
        f14dayregbackcnt,
        f30dayregbackcnt
        from analysis.user_back_pname_fct
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
a = agg_act_retained_reg_pname_day(statDate)
a()
