#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat
import service.sql_const as sql_const

class agg_reg_user_actret_month(BaseStat):

    def create_tab(self):
        hql = """create table if not exists dcnew.reg_user_actret_month
                (
                    fdate date,
                    fgamefsk bigint,
                    fplatformfsk bigint,
                    fhallfsk bigint,
                    fsubgamefsk bigint,
                    fterminaltypefsk bigint,
                    fversionfsk bigint,
                    fchannelcode bigint,
                    fmonthregcnt bigint,
                    f1monthcnt bigint,
                    f2monthcnt bigint,
                    f3monthcnt bigint
                )
                partitioned by (dt date);
                """
        res = self.hq.exe_sql(hql)
        if res != 0: return res



    def stat(self):
        query = {
            'statdate':self.stat_date,
            "num_date": self.stat_date.replace("-", ""),
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_str_report':sql_const.NULL_STR_REPORT,
            'null_int_report':sql_const.NULL_INT_REPORT
        }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        dates_dict = PublicFunc.date_define(self.stat_date)
        query.update(dates_dict)

        hql = """
        drop table if exists work.reg_user_actret_month_temp_main_%(num_date)s;
        create table if not exists work.reg_user_actret_month_temp_main_%(num_date)s
        as
        select fdate,fbpid,fchannel_code,fuid from
        (
        select "%(ld_3monthago_begin)s" fdate, fbpid,fchannel_code,fuid
        from dim.reg_user_main_additional
        where dt>='%(ld_3monthago_begin)s' and dt < '%(ld_3monthago_end)s'
        union all
        select "%(ld_2monthago_begin)s" fdate, fbpid,fchannel_code,fuid
        from dim.reg_user_main_additional
        where dt>='%(ld_2monthago_begin)s' and dt < '%(ld_2monthago_end)s'
        union all
        select "%(ld_1monthago_begin)s" fdate, fbpid,fchannel_code,fuid
        from dim.reg_user_main_additional
        where dt>='%(ld_1monthago_begin)s' and dt < '%(ld_1monthago_end)s'
        union all
        select "%(ld_monthbegin)s" fdate, fbpid,fchannel_code,fuid
        from dim.reg_user_main_additional
        where dt>='%(ld_monthbegin)s' and dt < '%(ld_monthend)s'
        ) a;

        drop table if exists work.reg_user_actret_month_temp_sub_%(num_date)s;
        create table if not exists work.reg_user_actret_month_temp_sub_%(num_date)s
        as
        select fdate,fbpid,fgame_id,fchannel_code,fuid from
        (
        select "%(ld_3monthago_begin)s" fdate, fbpid,fgame_id,fchannel_code,fuid
        from dim.reg_user_sub
        where dt>='%(ld_3monthago_begin)s' and dt < '%(ld_3monthago_end)s'
        and fis_first = 1
        union all
        select "%(ld_2monthago_begin)s" fdate, fbpid,fgame_id,fchannel_code,fuid
        from dim.reg_user_sub
        where dt>='%(ld_2monthago_begin)s' and dt < '%(ld_2monthago_end)s'
        and fis_first = 1
        union all
        select "%(ld_1monthago_begin)s" fdate, fbpid,fgame_id,fchannel_code,fuid
        from dim.reg_user_sub
        where dt>='%(ld_1monthago_begin)s' and dt < '%(ld_1monthago_end)s'
        and fis_first = 1
        union all
        select "%(ld_monthbegin)s" fdate, fbpid,fgame_id,fchannel_code,fuid
        from dim.reg_user_sub
        where dt>='%(ld_monthbegin)s' and dt < '%(ld_monthend)s'
        ) a;

        set hive.auto.convert.join=false;

        drop table if exists work.user_reatained_regmonth_final_%(num_date)s;
        create table if not exists work.user_reatained_regmonth_final_%(num_date)s
        as
        select
            tmp.fdate,
            bm.fgamefsk,
            coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
            %(null_int_group_rule)d fsubgamefsk,
            coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(tmp.fchannel_code,%(null_int_group_rule)d) fchannelcode,
            round(datediff('%(ld_monthbegin)s',tmp.fdate)/30) retday,
            count(distinct tmp.fuid) retusernum
        from work.reg_user_actret_month_temp_main_%(num_date)s tmp
        join dim.user_act ua
          on tmp.fbpid=ua.fbpid
         and tmp.fuid=ua.fuid
         and ua.dt >= '%(ld_monthbegin)s'
         and ua.dt < '%(ld_monthend)s'
         and ua.fgame_id = %(null_int_report)d
        join dim.bpid_map bm
         on tmp.fbpid = bm.fbpid
        group by bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,bm.fterminaltypefsk,bm.fversionfsk,
        tmp.fchannel_code,tmp.fdate
        grouping sets(
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,bm.fterminaltypefsk,bm.fversionfsk,tmp.fdate ),
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,tmp.fdate ),
            (bm.fgamefsk,bm.fplatformfsk,bm.fterminaltypefsk,bm.fversionfsk,tmp.fdate ),
            (bm.fgamefsk,bm.fplatformfsk,tmp.fdate))

        union all

        select
            tmp.fdate,
            bm.fgamefsk,
            coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
            coalesce(tmp.fgame_id,%(null_int_group_rule)d) fsubgamefsk,
            coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(tmp.fchannel_code,%(null_int_group_rule)d) fchannelcode,
            round(datediff('%(ld_monthbegin)s',tmp.fdate)/30) retday,
            count(distinct tmp.fuid) retusernum
        from work.reg_user_actret_month_temp_sub_%(num_date)s tmp
        join dim.user_act ua
          on tmp.fbpid=ua.fbpid
         and tmp.fuid=ua.fuid
         and ua.dt >= '%(ld_monthbegin)s'
         and ua.dt < '%(ld_monthend)s'
         and ua.fgame_id <> %(null_int_report)d
        join dim.bpid_map bm
         on tmp.fbpid = bm.fbpid
        group by bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,tmp.fgame_id,bm.fterminaltypefsk,bm.fversionfsk,
        tmp.fchannel_code,tmp.fdate
        grouping sets (
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,tmp.fgame_id,bm.fterminaltypefsk,bm.fversionfsk,tmp.fdate),
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,tmp.fgame_id,tmp.fdate),
            (bm.fgamefsk,tmp.fgame_id,tmp.fdate));

        insert overwrite table dcnew.reg_user_actret_month
        partition( dt )
        select
            fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
            max(fmonthregcnt) fmonthregcnt,
            max(f1monthcnt) f1monthcnt,
            max(f2monthcnt) f2monthcnt,
            max(f3monthcnt) f3monthcnt,
            fdate dt
        from (
            select
                cast (fdate as date) fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                0 fmonthregcnt,
                if( retday=1, retusernum, 0 ) f1monthcnt,
                if( retday=2, retusernum, 0 ) f2monthcnt,
                if( retday=3, retusernum, 0 ) f3monthcnt
            from work.user_reatained_regmonth_final_%(num_date)s

            union all

            select
                fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                0 fmonthregcnt, f1monthcnt, f2monthcnt, f3monthcnt
            from dcnew.reg_user_actret_month
            where dt >= '%(ld_3monthago_begin)s' and dt < '%(ld_dayend)s'

            union all

            select
                cast (fdate as date) fdate,
                bm.fgamefsk,
                coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                %(null_int_group_rule)d fsubgamefsk,
                coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(tmp.fchannel_code,%(null_int_group_rule)d) fchannelcode,
                count(distinct tmp.fuid) fmonthregcnt,
                0 f1monthcnt,0 f2monthcnt,0 f3monthcnt
            from work.reg_user_actret_month_temp_main_%(num_date)s tmp
            join dim.bpid_map bm
             on tmp.fbpid = bm.fbpid
            group by fdate,bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,
            bm.fterminaltypefsk,bm.fversionfsk,tmp.fchannel_code
            grouping sets (
                (fdate,bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,bm.fterminaltypefsk,bm.fversionfsk),
                (fdate,bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk),
                (fdate,bm.fgamefsk,bm.fplatformfsk,bm.fterminaltypefsk,bm.fversionfsk),
                (fdate,bm.fgamefsk,bm.fplatformfsk))

            union all

            select
                cast (fdate as date) fdate,
                bm.fgamefsk,
                coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(tmp.fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(tmp.fchannel_code,%(null_int_group_rule)d) fchannelcode,
                count(distinct tmp.fuid) fmonthregcnt,
                0 f1monthcnt,0 f2monthcnt,0 f3monthcnt
            from work.reg_user_actret_month_temp_sub_%(num_date)s tmp
            join dim.bpid_map bm
             on tmp.fbpid = bm.fbpid
            group by fdate,bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,tmp.fgame_id,bm.fterminaltypefsk,bm.fversionfsk,
            tmp.fchannel_code
            grouping sets (
                (fdate,bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,tmp.fgame_id,bm.fterminaltypefsk,bm.fversionfsk),
                (fdate,bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,tmp.fgame_id),
                (fdate,bm.fgamefsk,tmp.fgame_id))
        ) tmp
        group by fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
        insert overwrite table dcnew.reg_user_actret_month partition(dt='3000-01-01')
        select fdate        ,
            fgamefsk        ,
            fplatformfsk    ,
            fhallfsk        ,
            fsubgamefsk     ,
            fterminaltypefsk,
            fversionfsk     ,
            fchannelcode    ,
            fmonthregcnt    ,
            f1monthcnt      ,
            f2monthcnt      ,
            f3monthcnt
        from dcnew.reg_user_actret_month
        where dt >= '%(ld_3monthago_begin)s'
        and dt < '%(ld_dayend)s'
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        res = self.hq.exe_sql("""
            drop table if exists work.reg_user_actret_month_temp_main_%(num_date)s;
            drop table if exists work.reg_user_actret_month_temp_sub_%(num_date)s;
            drop table if exists work.user_reatained_regmonth_final_%(num_date)s;"""% query)
        if res != 0: return res


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
a = agg_reg_user_actret_month(statDate)
a()
