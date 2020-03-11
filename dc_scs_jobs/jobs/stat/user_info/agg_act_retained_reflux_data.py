#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_act_retained_reflux_data(BaseStat):
    """
    回流用户，在今日的留存
    """
    def create_tab(self):
        hql = """
        create table if not exists analysis.user_back_retained_fct
        (
            fdate          date,
            fgamefsk       bigint,
            fplatformfsk   bigint,
            fversionfsk    bigint,
            fterminalfsk   bigint,
            f1daycnt       bigint,
            f2daycnt       bigint,
            f3daycnt       bigint,
            f4daycnt       bigint,
            f5daycnt       bigint,
            f6daycnt       bigint,
            f7daycnt       bigint,
            f14daycnt      bigint,
            f30daycnt      bigint
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
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0: return res

        hql = """
        drop table if exists stage.user_back_retained_tmp;

        -- 最近30天的回流用户，在当日的活跃留存
        create table stage.user_back_retained_tmp
        as
        select /*+ MAPJOIN(c) */
            b.dt fdate, c.fgamefsk,c.fplatformfsk,c.fversionfsk,c.fterminalfsk,
            datediff(a.dt, b.dt) retday,
            count(distinct a.fuid) retusernum
        from stage.active_user_mid a
        join stage.reflux_user_mid b
        on a.fbpid = b.fbpid and a.fuid = b.fuid
            and b.dt >= '%(ld_30dayago)s' and b.dt < '%(ld_daybegin)s'
            and b.reflux = 7 and b.reflux_type = 'cycle'
        join analysis.bpid_platform_game_ver_map c
            on a.fbpid = c.fbpid
        where a.dt = '%(ld_daybegin)s'
        group by b.dt, c.fgamefsk,c.fplatformfsk,c.fversionfsk,c.fterminalfsk, datediff(a.dt, b.dt);


        insert overwrite table analysis.user_back_retained_fct
        partition( dt )
        select fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            max(f1daycnt)  f1daycnt,
            max(f2daycnt)  f2daycnt,
            max(f3daycnt)  f3daycnt,
            max(f4daycnt)  f4daycnt,
            max(f5daycnt)  f5daycnt,
            max(f6daycnt)  f6daycnt,
            max(f7daycnt)  f7daycnt,
            max(f14daycnt) f14daycnt,
            max(f30daycnt) f30daycnt,
            fdate  dt
        from
        (
            select fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
                if( retday=1,  retusernum, 0 ) f1daycnt,
                if( retday=2,  retusernum, 0 ) f2daycnt,
                if( retday=3,  retusernum, 0 ) f3daycnt,
                if( retday=4,  retusernum, 0 ) f4daycnt,
                if( retday=5,  retusernum, 0 ) f5daycnt,
                if( retday=6,  retusernum, 0 ) f6daycnt,
                if( retday=7,  retusernum, 0 ) f7daycnt,
                if( retday=14, retusernum, 0 ) f14daycnt,
                if( retday=30, retusernum, 0 ) f30daycnt
            from user_back_retained_tmp

            union all

            select  fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
                f1daycnt,f2daycnt,f3daycnt,f4daycnt,f5daycnt,f6daycnt,f7daycnt,f14daycnt,f30daycnt
            from analysis.user_back_retained_fct
            where dt >= '%(ld_30dayago)s' and dt < '%(ld_dayend)s'
        ) a
        group by fdate, fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.user_back_retained_fct partition(dt='3000-01-01')
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
        f30daycnt
        from analysis.user_back_retained_fct
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
a = agg_act_retained_reflux_data(statDate)
a()
