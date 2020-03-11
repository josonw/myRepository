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
GROUPSET2 = {'alias':['temp_alias', 'const_alias'],
             'field':['dt', "datediff('%(ld_daybegin)s', b.dt)"],
             'comb_value':[[1,1]]}

alias_dic = {'bpid_tbl_alias':'c.', 'src_tbl_alias':'a.', 'const_alias':'','temp_alias':'b.'}



class agg_reflux_user_actret(BaseStat):
    """
    回流用户，在今日的留存
    """
    def create_tab(self):
        hql = """
        create table if not exists dcnew.reflux_user_actret
        (
            fdate          date,
            fgamefsk       bigint,
            fplatformfsk   bigint,
            fhallfsk       bigint,
            fsubgamefsk     bigint,
            fterminaltypefsk bigint,
            fversionfsk    bigint,
            fchannelcode   bigint,
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
        dates_dict = PublicFunc.date_define(self.stat_date)
        alias_dic.update(dates_dict)

        query = { 'statdate':self.stat_date,"num_date": self.stat_date.replace("-", ""),
        'bpid_tbl_alias':alias_dic['bpid_tbl_alias'][:-1],'src_tbl_alias':alias_dic['src_tbl_alias'][:-1],
        'group_by':sql_const.extend_groupset(GROUPSET1, GROUPSET2)% alias_dic,
        'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
        'null_str_report':sql_const.NULL_STR_REPORT,'null_int_report':sql_const.NULL_INT_REPORT}

        query.update(dates_dict)

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        res = self.hq.exe_sql("""set hive.auto.convert.join=false;drop table if exists work.agg_user_reflux_retained_temp_%(num_date)s"""% query)
        if res != 0: return res


        hql = """
        -- 最近30天的回流用户，在当日的活跃留存
        create table work.agg_user_reflux_retained_temp_%(num_date)s
        as
        select /*+ MAPJOIN(c) */
            b.dt fdate,
            c.fgamefsk,
            coalesce(c.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(c.fhallfsk,%(null_int_group_rule)d) fhallfsk,
            coalesce(a.fgame_id,%(null_int_group_rule)d) fsubgamefsk,
            coalesce(c.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(c.fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(a.fchannel_code,%(null_int_group_rule)d) fchannelcode,
            datediff('%(ld_daybegin)s', b.dt) retday,
            count(distinct a.fuid) retusernum
        from dim.user_act a
        join dim.user_reflux b
        on a.fbpid = b.fbpid and a.fuid = b.fuid
            and b.dt >= '%(ld_30dayago)s' and b.dt < '%(ld_daybegin)s'
            and b.freflux = 7 and b.freflux_type = 'cycle'
        join dim.bpid_map c
            on a.fbpid = c.fbpid
        where a.dt = '%(ld_daybegin)s'
        %(group_by)s
        """% query
        res = self.hq.exe_sql(hql)
        if res != 0: return res



        hql = """
        insert overwrite table dcnew.reflux_user_actret
        partition( dt )
        select fdate,fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
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
            select fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                if( retday=1,  retusernum, 0 ) f1daycnt,
                if( retday=2,  retusernum, 0 ) f2daycnt,
                if( retday=3,  retusernum, 0 ) f3daycnt,
                if( retday=4,  retusernum, 0 ) f4daycnt,
                if( retday=5,  retusernum, 0 ) f5daycnt,
                if( retday=6,  retusernum, 0 ) f6daycnt,
                if( retday=7,  retusernum, 0 ) f7daycnt,
                if( retday=14, retusernum, 0 ) f14daycnt,
                if( retday=30, retusernum, 0 ) f30daycnt
            from work.agg_user_reflux_retained_temp_%(num_date)s

            union all

            select  fdate,fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                f1daycnt,f2daycnt,f3daycnt,f4daycnt,f5daycnt,f6daycnt,f7daycnt,f14daycnt,f30daycnt
            from dcnew.reflux_user_actret
            where dt >= '%(ld_30dayago)s' and dt < '%(ld_dayend)s'
        ) a
        group by fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


        hql = """
        insert overwrite table dcnew.reflux_user_actret partition(dt='3000-01-01')
        select
        fdate,
        fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
        f1daycnt,
        f2daycnt,
        f3daycnt,
        f4daycnt,
        f5daycnt,
        f6daycnt,
        f7daycnt,
        f14daycnt,
        f30daycnt
        from dcnew.reflux_user_actret
        where dt >= '%(ld_30dayago)s' and dt < '%(ld_dayend)s'
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        res = self.hq.exe_sql("""drop table if exists work.agg_user_reflux_retained_temp_%(num_date)s"""% query)
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
a = agg_reflux_user_actret(statDate)
a()
