#! /usr/local/python272/bin/python
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

class agg_gameparty_user_partynum_dis(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dcnew.gameparty_user_partynum_dis
            (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fhallfsk bigint,
            fsubgamefsk bigint,
            fterminaltypefsk bigint,
            fversionfsk bigint,
            fchannelcode bigint,
            fdimension varchar(32),
            feq0 bigint,
            feq1 bigint,
            fm2lq5 bigint,
            fm6lq10 bigint,
            fm10lq20 bigint,
            fm20lq30 bigint,
            fm30lq40 bigint,
            fm40lq50 bigint,
            fm50lq60 bigint,
            fm60lq70 bigint,
            fm70lq80 bigint,
            fm80lq90 bigint,
            fm90lq100 bigint,
            fm100lq150 bigint,
            fm150lq200 bigint,
            fm200lq300 bigint,
            fm300lq400 bigint,
            fm400lq500 bigint,
            fm500lq1000 bigint,
            fm1000 bigint
            )
            partitioned by (dt date);
        """

        res = self.hq.exe_sql(hql)


    def stat(self):
        alias_dic = {'bpid_tbl_alias':'bm.','src_tbl_alias':'ua.'}
        query = { 'statdate':self.stat_date,
            'convert_groupid_all':sql_const.HQL_CONVERT_GROUPID_ALL,
            'group_by_fuid_all':sql_const.HQL_GROUP_BY_FUID_ALL % {'bpid_tbl_alias':'bm.','src_tbl_alias':'gi.'},
            'group_by':sql_const.HQL_GROUP_BY_ALL % {'bpid_tbl_alias':'bm.','src_tbl_alias':'ua.'},
            'group_by_no_sub_game':sql_const.HQL_GROUP_BY_NO_SUB_GAME % alias_dic,
            'group_by_include_sub_game':sql_const.HQL_GROUP_BY_INCLUDE_SUB_GAME % alias_dic,
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_int_report':sql_const.NULL_INT_REPORT
            }

        # res = self.hq.exe_sql("""set mapreduce.job.name= %s; """%self.__class__.__name__)
        # if res != 0: return res

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.gameparty_user_partynum_dis
        partition(dt = '%(statdate)s' )

        select
            '%(statdate)s' fdate,
            un.fgamefsk,
            un.fplatformfsk,
            un.fhallfsk,
            un.fgame_id fsubgamefsk,
            un.fterminaltypefsk,
            un.fversionfsk,
            un.fchannel_code fchannelcode,
            'register' fdimension,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) = 0 then un.fuid else null end),0) feq0,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) = 1 then un.fuid else null end),0) feq1,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 2 and coalesce(giu.fparty_num,0) <= 5 then un.fuid else null end),0) fm2lq5,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 5 and coalesce(giu.fparty_num,0) <= 10 then un.fuid else null end),0) fm5lq10,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 10 and coalesce(giu.fparty_num,0) <= 20 then un.fuid else null end),0) fm10lq20,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 20 and coalesce(giu.fparty_num,0) <= 30 then un.fuid else null end),0) fm20lq30,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 30 and coalesce(giu.fparty_num,0) <= 40 then un.fuid else null end),0) fm30lq40,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 40 and coalesce(giu.fparty_num,0) <= 50 then un.fuid else null end),0) fm40lq50,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 50 and coalesce(giu.fparty_num,0) <= 60 then un.fuid else null end),0) fm50lq60,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 60 and coalesce(giu.fparty_num,0) <= 70 then un.fuid else null end),0) fm60lq70,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 70 and coalesce(giu.fparty_num,0) <= 80 then un.fuid else null end),0) fm70lq80,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 80 and coalesce(giu.fparty_num,0) <= 90 then un.fuid else null end),0) fm80lq90,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 90 and coalesce(giu.fparty_num,0) <= 100 then un.fuid else null end),0) fm90lq100,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 100 and coalesce(giu.fparty_num,0) <= 150 then un.fuid else null end),0) fm100lq150,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 150 and coalesce(giu.fparty_num,0) <= 200 then un.fuid else null end),0) fm150lq200,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 200 and coalesce(giu.fparty_num,0) <= 300 then un.fuid else null end),0) fm200lq300,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 300 and coalesce(giu.fparty_num,0) <= 400 then un.fuid else null end),0) fm300lq400,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 400 and coalesce(giu.fparty_num,0) <= 500 then un.fuid else null end),0) fm400lq500,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 500 and coalesce(giu.fparty_num,0) <= 1000 then un.fuid else null end),0) fm500lq1000,
            coalesce(count(distinct case when coalesce(giu.fparty_num,0) > 1000 then un.fuid else null end),0) fm1000
        from
            dim.reg_user_array un

        left join
            (select
                bm.fgamefsk,
                coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(gi.fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(gi.fchannel_code,%(null_int_group_rule)d) fchannel_code,
                fuid,
                sum(gi.fparty_num) fparty_num
            from
                dim.user_gameparty gi
            join
                dim.bpid_map bm
            on
                gi.fbpid = bm.fbpid
            where dt = '%(statdate)s'
            %(group_by_fuid_all)s) giu
        on un.fuid = giu.fuid
            and un.fgamefsk = giu.fgamefsk
            and un.fplatformfsk = giu.fplatformfsk
            and un.fhallfsk = giu.fhallfsk
            and un.fgame_id = giu.fgame_id
            and un.fterminaltypefsk = giu.fterminaltypefsk
            and un.fversionfsk = giu.fversionfsk
            and un.fchannel_code = giu.fchannel_code

        where
            un.dt = '%(statdate)s'
        group by un.fgamefsk,un.fplatformfsk,un.fhallfsk,un.fgame_id,un.fterminaltypefsk,un.fversionfsk,un.fchannel_code

        union all

        select
            fdate,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fsubgamefsk,
            fterminaltypefsk,
            fversionfsk,
            fchannelcode,
            fdimension,
            feq0,
            feq1,
            fm2lq5,
            fm5lq10,
            fm10lq20,
            fm20lq30,
            fm30lq40,
            fm40lq50,
            fm50lq60,
            fm60lq70,
            fm70lq80,
            fm80lq90,
            fm90lq100,
            fm100lq150,
            fm150lq200,
            fm200lq300,
            fm300lq400,
            fm400lq500,
            fm500lq1000,
            fm1000
        from
            (select
                '%(statdate)s' fdate,
                bm.fgamefsk,
                coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(ua.fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(ua.fchannel_code,%(null_int_group_rule)d) fchannelcode,
                'active' fdimension,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) = 0 then ua.fuid else null end),0) feq0,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) = 1 then ua.fuid else null end),0) feq1,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 2 and coalesce(gi.fparty_num,0) <= 5 then ua.fuid else null end),0) fm2lq5,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 5 and coalesce(gi.fparty_num,0) <= 10 then ua.fuid else null end),0) fm5lq10,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 10 and coalesce(gi.fparty_num,0) <= 20 then ua.fuid else null end),0) fm10lq20,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 20 and coalesce(gi.fparty_num,0) <= 30 then ua.fuid else null end),0) fm20lq30,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 30 and coalesce(gi.fparty_num,0) <= 40 then ua.fuid else null end),0) fm30lq40,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 40 and coalesce(gi.fparty_num,0) <= 50 then ua.fuid else null end),0) fm40lq50,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 50 and coalesce(gi.fparty_num,0) <= 60 then ua.fuid else null end),0) fm50lq60,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 60 and coalesce(gi.fparty_num,0) <= 70 then ua.fuid else null end),0) fm60lq70,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 70 and coalesce(gi.fparty_num,0) <= 80 then ua.fuid else null end),0) fm70lq80,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 80 and coalesce(gi.fparty_num,0) <= 90 then ua.fuid else null end),0) fm80lq90,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 90 and coalesce(gi.fparty_num,0) <= 100 then ua.fuid else null end),0) fm90lq100,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 100 and coalesce(gi.fparty_num,0) <= 150 then ua.fuid else null end),0) fm100lq150,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 150 and coalesce(gi.fparty_num,0) <= 200 then ua.fuid else null end),0) fm150lq200,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 200 and coalesce(gi.fparty_num,0) <= 300 then ua.fuid else null end),0) fm200lq300,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 300 and coalesce(gi.fparty_num,0) <= 400 then ua.fuid else null end),0) fm300lq400,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 400 and coalesce(gi.fparty_num,0) <= 500 then ua.fuid else null end),0) fm400lq500,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 500 and coalesce(gi.fparty_num,0) <= 1000 then ua.fuid else null end),0) fm500lq1000,
                coalesce(count(distinct case when coalesce(gi.fparty_num,0) > 1000 then ua.fuid else null end),0) fm1000
            from
                dim.user_act ua
            left join
                (select fbpid,fgame_id,fuid,sum(fparty_num) fparty_num from dim.user_gameparty
                where dt = '%(statdate)s'
                group by fbpid,fgame_id,fuid) gi
            on ua.fbpid = gi.fbpid
                and ua.fuid = gi.fuid and ua.fgame_id = gi.fgame_id
            join dim.bpid_map bm
            on ua.fbpid = bm.fbpid
            where ua.dt = '%(statdate)s'
            %(group_by)s) src
        where fsubgamefsk <> %(null_int_report)d;
        """ % query

        res = self.hq.exe_sql(hql)
        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话,日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取,实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_gameparty_user_partynum_dis(statDate)
a()
