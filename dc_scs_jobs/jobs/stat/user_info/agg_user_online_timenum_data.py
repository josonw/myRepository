#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_online_timenum_data(BaseStat):
    """ 用户来源，广告用户，feed，push
    """
    def create_tab(self):
        hql = """
        create table if not exists analysis.user_online_timenum_fct
        (
            fdate         date,
            fgamefsk      bigint,
            fplatformfsk  bigint,
            fversionfsk   bigint,
            fterminalfsk  bigint,
            online_time   varchar(20),
            fusernum      bigint,
            fpayusernum   bigint,
            fpplaycnt     bigint,
            fplaycnt      bigint
        )
        partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict""")
        if res != 0:
            return res

        hql = """
        insert overwrite table analysis.user_online_timenum_fct
        partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            fterminalfsk,
            online_time,
            max(fusernum) fusernum,
            max(fpayusernum) fpayusernum,
            max(fpplaycnt) fpplaycnt,
            max(fplaycnt) fplaycnt
        from
        (
            select fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                online_time,
                count(distinct a.fuid) fusernum,
                count(distinct b.fplatform_uid) fpayusernum,
                null fpplaycnt,
                null fplaycnt
            from
            (
                -- select fday_at fdate, fbpid, fuid,
                select fbpid, fuid,
                case when fseconds >= 0 and fseconds < 300 then '0-5分钟'
                    when fseconds >= 300 and fseconds < 900 then  '5-15分钟'
                    when fseconds >= 900 and fseconds < 1800 then '15-30分钟'
                    when fseconds >= 1800 and fseconds < 3600 then '30-60分钟'
                    when fseconds >= 3600 and fseconds < 7200 then '1-2小时'
                    when fseconds >= 7200 and fseconds < 14400 then '2-4小时'
                    when fseconds >= 14400 and fseconds < 28800 then '4-8小时'
                else '8-24小时' end online_time
                from
                (
                    select fbpid, fuid, sum(fseconds) fseconds
                    from stage.user_daytime_stg a
                    where a.dt = '%(ld_daybegin)s'
                        and length(fday_at)=10
                    group by fbpid, fuid
                ) tmp
            ) a
            left join stage.pay_user_mid b
                on a.fbpid = b.fbpid and a.fuid = b.fuid
            join analysis.bpid_platform_game_ver_map c
                on a.fbpid = c.fbpid
            group by fgamefsk,
                fplatformfsk,
                fversionfsk,
                fterminalfsk,
                online_time

            union all

            select b.fgamefsk,
                b.fplatformfsk,
                b.fversionfsk,
                b.fterminalfsk,
                a.online_time,
                null fusernum,
                null fpayusernum,
                count(distinct if(ffirst_pay_income is not null, fuid, null)) fpplaycnt,
                count(distinct fuid) fplaycnt
            from
            (
                -- select a.fdate, a.fbpid, a.fuid,
                select a.fbpid, a.fuid,
                    sum(b.ffirst_pay_income) ffirst_pay_income,
                    case when sum(a.fplaytime) >= 0 and sum(a.fplaytime) < 300 then '0-5分钟'
                    when sum(a.fplaytime) >= 300 and sum(a.fplaytime) < 900 then '5-15分钟'
                    when sum(a.fplaytime) >= 900 and sum(a.fplaytime) < 1800 then '15-30分钟'
                    when sum(a.fplaytime) >= 1800 and sum(a.fplaytime) < 3600 then '30-60分钟'
                    when sum(a.fplaytime) >= 3600 and sum(a.fplaytime) < 7200 then '1-2小时'
                    when sum(a.fplaytime) >= 7200 and sum(a.fplaytime) < 14400 then '2-4小时'
                    when sum(a.fplaytime) >= 14400 and sum(a.fplaytime) < 28800 then '4-8小时'
                    else '8-24小时' end online_time
                from stage.user_gameparty_info_mid a
                left outer join
                (
                    -- 这个表的fbpid, fuid会重复
                    select fbpid, fuid, sum(ffirst_pay_income) as ffirst_pay_income
                    from stage.pay_user_mid
                    group by fbpid, fuid
                ) b
                on a.fbpid = b.fbpid and a.fuid = b.fuid
                where a.dt = '%(ld_daybegin)s'
                -- group by a.fdate, a.fbpid, a.fuid
                group by a.fbpid, a.fuid
            ) a
            join analysis.bpid_platform_game_ver_map b
                on a.fbpid = b.fbpid
            group by b.fgamefsk,
                b.fplatformfsk,
                b.fversionfsk,
                b.fterminalfsk,
                a.online_time
        ) tmp
        group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, online_time
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
a = agg_user_online_timenum_data(statDate)
a()
