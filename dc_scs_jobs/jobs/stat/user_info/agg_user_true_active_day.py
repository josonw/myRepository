#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_true_active_day(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """create table if not exists analysis.user_true_active_fct
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                factcnt bigint,
                f3dayactcnt bigint,
                f7dayactcnt bigint,
                f14dayactcnt bigint,
                f30dayactcnt bigint,
                fweekactcnt bigint,
                fmonthactcnt bigint,
                fregcnt bigint,
                fmau_stay bigint,       --月活跃留存
                fmau_back bigint,       --月活跃回流用户
                fmau_hback bigint,     --
                fmau_new bigint,        --月活跃新用户
                fmau_lose bigint,      --月活跃流失用户
                fmau bigint,           --月活跃用户
                flmau_new bigint,
                flmau_new_stay bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """create table if not exists stage.user_true_active_tmp
                (
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fnum     bigint
                )
                partitioned by (dt date, fdim string)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

    def stat(self):
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0: return res
        # self.hq.debug = 0
        hql = """
        drop table if exists stage.user_true_active_user_tmp;
        create table stage.user_true_active_user_tmp
        as
        select fbpid, fuid, max(dt) dt
        from stage.active_user_mid
        where dt >= '%(ld_begin)s'
        and dt < '%(ld_dayend)s'
        and fuid is not null
        group by fbpid, fuid;

        from (select bpm.fgamefsk, bpm.fplatformfsk, bpm.fversion_old fversionfsk, bpm.fterminalfsk, fuid, dt
              from stage.user_true_active_user_tmp uls
              join dim.bpid_map bpm
                  on bpm.fbpid = uls.fbpid) a
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='factcnt')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct fuid) factcnt where a.dt='%(ld_daybegin)s'
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='f3dayactcnt')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct fuid) factcnt where a.dt >= '%(ld_2dayago)s' and a.dt < '%(ld_dayend)s'
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='f7dayactcnt')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct fuid) factcnt where a.dt >= '%(ld_6dayago)s' and a.dt < '%(ld_dayend)s'
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='f14dayactcnt')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct fuid) factcnt where a.dt >= '%(ld_13dayago)s' and a.dt < '%(ld_dayend)s'
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='f30dayactcnt')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct fuid) factcnt where a.dt >= '%(ld_29dayago)s' and a.dt < '%(ld_dayend)s'
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='fweekactcnt')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct fuid) factcnt where a.dt >= '%(ld_weekbegin)s' and a.dt < '%(ld_dayend)s'
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='fmonthactcnt')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct fuid) factcnt where a.dt >= '%(ld_monthbegin)s' and a.dt < '%(ld_dayend)s'
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;

        insert overwrite table stage.user_true_active_tmp
        partition(dt='%(ld_daybegin)s', fdim='fregcnt')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, sum(fregcnt) fregcnt
          from analysis.user_register_fct ur
        where ur.dt='%(ld_daybegin)s'
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;

        drop table if exists stage.user_true_active_user_tmp;
        create table stage.user_true_active_user_tmp
        as
        select fbpid, fuid, max(tmonth) tmonth, max(lmonth) lmonth,
                max(llmonth) llmonth, max(is_new) is_new, max(lis_new) lis_new
        from (
            select fbpid, fuid, 1 tmonth, 0 lmonth, 0 llmonth, 0 is_new, 0 lis_new
            from stage.active_user_mid where dt >= '%(ld_monthbegin)s' and dt < '%(ld_dayend)s'
            union all
            select fbpid, fuid, 0 tmonth, 1 lmonth, 0 llmonth, 0 is_new, 0 lis_new
            from stage.active_user_mid where dt >= '%(ld_1monthago_begin)s' and dt < '%(ld_monthbegin)s'
            union all
            select fbpid, fuid, 0 tmonth, 0 lmonth, 1 llmonth, 0 is_new, 0 lis_new
            from stage.active_user_mid where dt >= '%(ld_2monthago_begin)s' and dt < '%(ld_1monthago_begin)s'
            union all
            select fbpid, fuid, 0 tmonth, 0 lmonth, 0 llmonth,
              case when dt >= '%(ld_monthbegin)s' and dt < '%(ld_dayend)s' then 1 else 0 end is_new,
              case when dt >= '%(ld_1monthago_begin)s' and dt < '%(ld_monthbegin)s' then 1 else 0 end lis_new
            from stage.user_dim b
            where dt >= '%(ld_1monthago_begin)s' and dt < '%(ld_dayend)s'
        ) t
        where fuid is not null
        group by fbpid, fuid;



        from (select bpm.fgamefsk, bpm.fplatformfsk, bpm.fversion_old fversionfsk, bpm.fterminalfsk,
                    tmonth, lmonth, llmonth, is_new, lis_new, fuid actnum
              from stage.user_true_active_user_tmp uls
              join dim.bpid_map bpm
                  on bpm.fbpid = uls.fbpid
              ) a
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='fmau')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct actnum) fnum where tmonth=1
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='fmau_new')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct actnum) fnum where tmonth=1 and is_new=1 and lmonth=0 and llmonth=0
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='fmau_stay')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct actnum) fnum where tmonth=1 and lmonth=1
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='fmau_back')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct actnum) fnum where tmonth=1 and lmonth=0 and llmonth=1
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='fmau_hback')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct actnum) fnum where tmonth=1 and lmonth=0 and llmonth=0 and is_new=0
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='fmau_lose')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct actnum) fnum where tmonth=0 and lmonth=1
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='flmau_new')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct actnum) fnum where lmonth=1 and lis_new=1
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk
        insert overwrite table stage.user_true_active_tmp partition(dt='%(ld_daybegin)s', fdim='flmau_new_stay')
        select fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, count(distinct actnum) fnum where tmonth=1 and lmonth=1 and lis_new=1
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;


        insert overwrite table analysis.user_true_active_fct
        partition (dt = '%(ld_daybegin)s')
        select  '%(ld_daybegin)s' fdate,
                fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                max(if(fdim='factcnt', fnum, 0))  factcnt,
                max(if(fdim='f3dayactcnt', fnum, 0))  f3dayactcnt,
                max(if(fdim='f7dayactcnt', fnum, 0))  f7dayactcnt,
                max(if(fdim='f14dayactcnt', fnum, 0))  f14dayactcnt,
                max(if(fdim='f30dayactcnt', fnum, 0))  f30dayactcnt,
                max(if(fdim='fweekactcnt', fnum, 0))  fweekactcnt,
                max(if(fdim='fmonthactcnt', fnum, 0))  fmonthactcnt,
                max(if(fdim='fregcnt', fnum, 0))  fregcnt,
                max(if(fdim='fmau_stay', fnum, 0)) fmau_stay,
                max(if(fdim='fmau_back', fnum, 0)) fmau_back,
                max(if(fdim='fmau_hback', fnum, 0)) fmau_hback,
                max(if(fdim='fmau_new', fnum, 0)) fmau_new,
                max(if(fdim='fmau_lose', fnum, 0)) fmau_lose,
                max(if(fdim='fmau', fnum, 0)) fmau,
                max(if(fdim='flmau_new', fnum, 0)) flmau_new,
                max(if(fdim='flmau_new_stay', fnum, 0)) flmau_new_stay
        from user_true_active_tmp where dt='%(ld_daybegin)s'
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk;
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
a = agg_user_true_active_day(statDate)
a()
