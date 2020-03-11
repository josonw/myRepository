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

class agg_reg_user_actret_week(BaseStat):

    def create_tab(self):
        hql = """create table if not exists dcnew.reg_user_actret_week
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fhallfsk bigint,
                fsubgamefsk bigint,
                fterminaltypefsk bigint,
                fversionfsk bigint,
                fchannelcode bigint,
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
        drop table if exists work.reg_user_actret_week_temp_main_%(num_date)s;
        create table if not exists work.reg_user_actret_week_temp_main_%(num_date)s
        as
        select fdate,fbpid,fchannel_code,fuid from
        (
        select '%(ld_8weekago_begin)s' fdate, fbpid,fchannel_code,fuid
        from dim.reg_user_main_additional
        where dt>='%(ld_8weekago_begin)s' and dt < '%(ld_8weekago_end)s'
        union all
        select '%(ld_7weekago_begin)s' fdate, fbpid,fchannel_code,fuid
        from dim.reg_user_main_additional
        where dt>='%(ld_7weekago_begin)s' and dt < '%(ld_7weekago_end)s'
        union all
        select '%(ld_6weekago_begin)s' fdate, fbpid,fchannel_code,fuid
        from dim.reg_user_main_additional
        where dt>='%(ld_6weekago_begin)s' and dt < '%(ld_6weekago_end)s'
        union all
        select '%(ld_5weekago_begin)s' fdate, fbpid,fchannel_code,fuid
        from dim.reg_user_main_additional
        where dt>='%(ld_5weekago_begin)s' and dt < '%(ld_5weekago_end)s'
        union all
        select '%(ld_4weekago_begin)s' fdate, fbpid,fchannel_code,fuid
        from dim.reg_user_main_additional
        where dt>='%(ld_4weekago_begin)s' and dt < '%(ld_4weekago_end)s'
        union all
        select '%(ld_3weekago_begin)s' fdate, fbpid,fchannel_code,fuid
        from dim.reg_user_main_additional
        where dt>='%(ld_3weekago_begin)s' and dt < '%(ld_3weekago_end)s'
        union all
        select '%(ld_2weekago_begin)s' fdate, fbpid,fchannel_code,fuid
        from dim.reg_user_main_additional
        where dt>='%(ld_2weekago_begin)s' and dt < '%(ld_2weekago_end)s'
        union all
        select '%(ld_1weekago_begin)s' fdate, fbpid,fchannel_code,fuid
        from dim.reg_user_main_additional
        where dt>='%(ld_1weekago_begin)s' and dt < '%(ld_1weekago_end)s'
        union all
        select '%(ld_weekbegin)s' fdate, fbpid,fchannel_code,fuid
        from dim.reg_user_main_additional
        where dt>='%(ld_weekbegin)s' and dt < '%(ld_weekend)s'
        ) a;

        drop table if exists work.reg_user_actret_week_temp_sub_%(num_date)s;
        create table if not exists work.reg_user_actret_week_temp_sub_%(num_date)s
        as
        select fdate,fbpid,fgame_id,fchannel_code,fuid from
        (
        select '%(ld_8weekago_begin)s' fdate, fbpid,fgame_id,fchannel_code,fuid
        from dim.reg_user_sub
        where dt>='%(ld_8weekago_begin)s' and dt < '%(ld_8weekago_end)s'
        and fis_first = 1
        union all
        select '%(ld_7weekago_begin)s' fdate, fbpid,fgame_id,fchannel_code,fuid
        from dim.reg_user_sub
        where dt>='%(ld_7weekago_begin)s' and dt < '%(ld_7weekago_end)s'
        and fis_first = 1
        union all
        select '%(ld_6weekago_begin)s' fdate, fbpid,fgame_id,fchannel_code,fuid
        from dim.reg_user_sub
        where dt>='%(ld_6weekago_begin)s' and dt < '%(ld_6weekago_end)s'
        and fis_first = 1
        union all
        select '%(ld_5weekago_begin)s' fdate, fbpid,fgame_id,fchannel_code,fuid
        from dim.reg_user_sub
        where dt>='%(ld_5weekago_begin)s' and dt < '%(ld_5weekago_end)s'
        and fis_first = 1
        union all
        select '%(ld_4weekago_begin)s' fdate, fbpid,fgame_id,fchannel_code,fuid
        from dim.reg_user_sub
        where dt>='%(ld_4weekago_begin)s' and dt < '%(ld_4weekago_end)s'
        and fis_first = 1
        union all
        select '%(ld_3weekago_begin)s' fdate, fbpid,fgame_id,fchannel_code,fuid
        from dim.reg_user_sub
        where dt>='%(ld_3weekago_begin)s' and dt < '%(ld_3weekago_end)s'
        and fis_first = 1
        union all
        select '%(ld_2weekago_begin)s' fdate, fbpid,fgame_id,fchannel_code,fuid
        from dim.reg_user_sub
        where dt>='%(ld_2weekago_begin)s' and dt < '%(ld_2weekago_end)s'
        and fis_first = 1
        union all
        select '%(ld_1weekago_begin)s' fdate, fbpid,fgame_id,fchannel_code,fuid
        from dim.reg_user_sub
        where dt>='%(ld_1weekago_begin)s' and dt < '%(ld_1weekago_end)s'
        and fis_first = 1
        union all
        select '%(ld_weekbegin)s' fdate, fbpid,fgame_id,fchannel_code,fuid
        from dim.reg_user_sub
        where dt>='%(ld_weekbegin)s' and dt < '%(ld_weekend)s'
        ) a;

        set hive.auto.convert.join=false;

        drop table if exists work.user_reatained_regweek_final_%(num_date)s;
        create table if not exists work.user_reatained_regweek_final_%(num_date)s
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
            datediff('%(ld_weekbegin)s', tmp.fdate)/7 retday,
            count(distinct tmp.fuid) retusernum
        from work.reg_user_actret_week_temp_main_%(num_date)s tmp
        join dim.user_act ua
          on tmp.fbpid=ua.fbpid
         and tmp.fuid=ua.fuid
         and ua.dt >= '%(ld_weekbegin)s'
         and ua.dt < '%(ld_weekend)s'
         and ua.fgame_id = %(null_int_report)d
        join dim.bpid_map bm
         on tmp.fbpid = bm.fbpid
        group by bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,bm.fterminaltypefsk,bm.fversionfsk,
        tmp.fchannel_code,tmp.fdate
        grouping sets (
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
            datediff('%(ld_weekbegin)s', tmp.fdate)/7 retday,
            count(distinct tmp.fuid) retusernum
        from work.reg_user_actret_week_temp_sub_%(num_date)s tmp
        join dim.user_act ua
          on tmp.fbpid=ua.fbpid
         and tmp.fuid=ua.fuid
         and ua.dt >= '%(ld_weekbegin)s'
         and ua.dt < '%(ld_weekend)s'
         and ua.fgame_id <> %(null_int_report)d
        join dim.bpid_map bm
         on tmp.fbpid = bm.fbpid
        group by bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,tmp.fgame_id,bm.fterminaltypefsk,bm.fversionfsk,
        tmp.fchannel_code,tmp.fdate
        grouping sets (
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,tmp.fgame_id,bm.fterminaltypefsk,bm.fversionfsk,tmp.fdate),
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,tmp.fgame_id,tmp.fdate),
            (bm.fgamefsk,tmp.fgame_id,tmp.fdate));

        insert overwrite table dcnew.reg_user_actret_week
        partition( dt )
        select
            fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
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
            select
                cast (fdate as date) fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                0 fweekregcnt,
                if( retday=1, retusernum, 0 ) f1weekcnt,
                if( retday=2, retusernum, 0 ) f2weekcnt,
                if( retday=3, retusernum, 0 ) f3weekcnt,
                if( retday=4, retusernum, 0 ) f4weekcnt,
                if( retday=5, retusernum, 0 ) f5weekcnt,
                if( retday=6, retusernum, 0 ) f6weekcnt,
                if( retday=7, retusernum, 0 ) f7weekcnt,
                if( retday=8, retusernum, 0 ) f8weekcnt
            from work.user_reatained_regweek_final_%(num_date)s

            union all

            select
                fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                0 fweekregcnt, f1weekcnt, f2weekcnt, f3weekcnt, f4weekcnt,
                f5weekcnt, f6weekcnt, f7weekcnt, f8weekcnt
            from dcnew.reg_user_actret_week
            where dt >= '%(ld_8weekago_begin)s' and dt < '%(ld_dayend)s'

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
                count(distinct tmp.fuid) fweekregcnt,
                0 f1weekcnt,0 f2weekcnt,0 f3weekcnt,0 f4weekcnt,0 f5weekcnt,0 f6weekcnt,0 f7weekcnt,0 f8weekcnt
            from work.reg_user_actret_week_temp_main_%(num_date)s tmp
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
                count(distinct tmp.fuid) fweekregcnt,
                0 f1weekcnt,0 f2weekcnt,0 f3weekcnt,0 f4weekcnt,0 f5weekcnt,0 f6weekcnt,0 f7weekcnt,0 f8weekcnt
            from work.reg_user_actret_week_temp_sub_%(num_date)s tmp
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

        res = self.hq.exe_sql("""
        insert overwrite table dcnew.reg_user_actret_week
        partition(dt='3000-01-01')
        select fdate ,
                fgamefsk ,
                fplatformfsk ,
                fhallfsk ,
                fsubgamefsk ,
                fterminaltypefsk ,
                fversionfsk ,
                fchannelcode ,
                fweekregcnt ,
                f1weekcnt ,
                f2weekcnt ,
                f3weekcnt ,
                f4weekcnt ,
                f5weekcnt ,
                f6weekcnt ,
                f7weekcnt ,
                f8weekcnt
        from dcnew.reg_user_actret_week
        where dt >= '%(ld_8weekago_begin)s' and dt < '%(ld_dayend)s'
        """% query)
        if res != 0: return res

        res = self.hq.exe_sql("""
            drop table if exists work.reg_user_actret_week_temp_main_%(num_date)s;
            drop table if exists work.reg_user_actret_week_temp_sub_%(num_date)s;
            drop table if exists work.user_reatained_regweek_final_%(num_date)s;"""% query)
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
a = agg_reg_user_actret_week(statDate)
a()
