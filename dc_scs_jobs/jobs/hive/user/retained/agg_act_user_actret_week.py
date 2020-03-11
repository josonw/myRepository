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



GROUPSET1 = sql_const.GROUPSET
GROUPSET2 = {'alias':['src_tbl_alias', 'const_alias'],
             'field':['fdate', "datediff('%(ld_weekbegin)s', a.fdate)/7"],
             'comb_value':[[1,1]]}


GROUPSET3 = {'alias':['src_tbl_alias'],
             'field':['fdate'],
             'comb_value':[[1]]}


alias_dic = {'bpid_tbl_alias':'bgm.', 'src_tbl_alias':'a.', 'const_alias':''}


class agg_act_user_actret_week(BaseStat):
    """活跃用户，活跃留存，自然周
    """

    def create_tab(self):
        hql = """create table if not exists dcnew.act_user_actret_week
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fhallfsk bigint,
                fsubgamefsk bigint,
                fterminaltypefsk bigint,
                fversionfsk bigint,
                fchannelcode bigint,
                fweekaucnt bigint,
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
        dates_dict = PublicFunc.date_define(self.stat_date)
        alias_dic.update(dates_dict)

        query = { 'statdate':self.stat_date,"num_date": self.stat_date.replace("-", ""),
        'bpid_tbl_alias':alias_dic['bpid_tbl_alias'][:-1],'src_tbl_alias':alias_dic['src_tbl_alias'][:-1],
        'group_by':sql_const.extend_groupset(GROUPSET1, GROUPSET2)% alias_dic,
        'group_by1':sql_const.extend_groupset(GROUPSET1, GROUPSET3)% alias_dic,
        'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
        'null_str_report':sql_const.NULL_STR_REPORT,'null_int_report':sql_const.NULL_INT_REPORT}
        query.update(dates_dict)

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        res = self.hq.exe_sql("""drop table if exists work.act_user_actret_week_temp_%(num_date)s""" %query)
        if res != 0: return res


        hql= """
        create table if not exists work.act_user_actret_week_temp_%(num_date)s
        as
        select
        distinct
        case when dt>='%(ld_8weekago_begin)s' and dt < '%(ld_8weekago_end)s' then '%(ld_8weekago_begin)s'
             when dt>='%(ld_7weekago_begin)s' and dt < '%(ld_7weekago_end)s' then '%(ld_7weekago_begin)s'
             when dt>='%(ld_6weekago_begin)s' and dt < '%(ld_6weekago_end)s' then '%(ld_6weekago_begin)s'
             when dt>='%(ld_5weekago_begin)s' and dt < '%(ld_5weekago_end)s' then '%(ld_5weekago_begin)s'
             when dt>='%(ld_4weekago_begin)s' and dt < '%(ld_4weekago_end)s' then '%(ld_4weekago_begin)s'
             when dt>='%(ld_3weekago_begin)s' and dt < '%(ld_3weekago_end)s' then '%(ld_3weekago_begin)s'
             when dt>='%(ld_2weekago_begin)s' and dt < '%(ld_2weekago_end)s' then '%(ld_2weekago_begin)s'
             when dt>='%(ld_1weekago_begin)s' and dt < '%(ld_1weekago_end)s' then '%(ld_1weekago_begin)s'
             when dt>='%(ld_weekbegin)s' and dt < '%(ld_weekend)s' then '%(ld_weekbegin)s'
        end fdate, fbpid, fuid, fgame_id, fchannel_code
        from dim.user_act
        where dt>='%(ld_8weekago_begin)s' and dt < '%(ld_weekend)s' """%query

        res = self.hq.exe_sql(hql)
        if res != 0: return res


        res = self.hq.exe_sql("""drop table if exists work.agg_user_retained_week_temp_%(num_date)s"""%query)
        if res != 0: return res


        hql="""
        create table work.agg_user_retained_week_temp_%(num_date)s
        as
        select a.fdate,
                bgm.fgamefsk,
                coalesce(bgm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(bgm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(a.fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(bgm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(bgm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(a.fchannel_code,%(null_int_group_rule)d) fchannelcode,
                datediff('%(ld_weekbegin)s', a.fdate)/7 retday,
        count(distinct a.fuid) retusernum
        from work.act_user_actret_week_temp_%(num_date)s a
        join dim.user_act b
          on a.fbpid=b.fbpid
         and a.fuid=b.fuid
         and b.dt >= '%(ld_weekbegin)s'
         and b.dt < '%(ld_weekend)s'
        join dim.bpid_map bgm
        on a.fbpid = bgm.fbpid
        %(group_by)s"""%query

        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table dcnew.act_user_actret_week
        partition( dt )
        select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
            max(fweekaucnt) fweekaucnt,
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
            select cast (fdate as date) fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                0 fweekaucnt,
                if( retday=1, retusernum, 0 ) f1weekcnt,
                if( retday=2, retusernum, 0 ) f2weekcnt,
                if( retday=3, retusernum, 0 ) f3weekcnt,
                if( retday=4, retusernum, 0 ) f4weekcnt,
                if( retday=5, retusernum, 0 ) f5weekcnt,
                if( retday=6, retusernum, 0 ) f6weekcnt,
                if( retday=7, retusernum, 0 ) f7weekcnt,
                if( retday=8, retusernum, 0 ) f8weekcnt
            from work.agg_user_retained_week_temp_%(num_date)s

            union all

            select  fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
            0 fweekaucnt, f1weekcnt, f2weekcnt, f3weekcnt, f4weekcnt, f5weekcnt, f6weekcnt, f7weekcnt, f8weekcnt
            from dcnew.act_user_actret_week
            where dt >= '%(ld_8weekago_begin)s' and dt < '%(ld_dayend)s'

            union all

            select cast (a.fdate as date) fdate,bgm.fgamefsk,
                coalesce(bgm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(bgm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(a.fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(bgm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(bgm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(a.fchannel_code,%(null_int_group_rule)d) fchannelcode,
            count(distinct a.fuid) fweekaucnt,0 f1weekcnt,0 f2weekcnt,0 f3weekcnt,0 f4weekcnt,0 f5weekcnt,0 f6weekcnt,0 f7weekcnt,0 f8weekcnt
            from work.act_user_actret_week_temp_%(num_date)s a
            join dim.bpid_map bgm
            on a.fbpid = bgm.fbpid
            %(group_by)s
        ) tmp group by fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table dcnew.act_user_actret_week partition(dt='3000-01-01')
        select
        fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
        fweekaucnt,
        f1weekcnt,
        f2weekcnt,
        f3weekcnt,
        f4weekcnt,
        f5weekcnt,
        f6weekcnt,
        f7weekcnt,
        f8weekcnt
        from dcnew.act_user_actret_week
        where dt >= '%(ld_8weekago_begin)s' and dt < '%(ld_dayend)s'
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        res = self.hq.exe_sql("""drop table if exists work.agg_user_retained_week_temp_%(num_date)s"""%query)
        if res != 0: return res

        res = self.hq.exe_sql("""drop table if exists work.act_user_actret_week_temp_%(num_date)s""" %query)
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
a = agg_act_user_actret_week(statDate)
a()
