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

class agg_bankrupt_user_cnt_dis(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dcnew.bankrupt_user_cnt_dis
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
            feq1 bigint,
            feq2 bigint,
            feq3 bigint,
            feq4 bigint,
            feq5 bigint,
            fm5 bigint
            )
            partitioned by (dt date);
        """

        res = self.hq.exe_sql(hql)


    def stat(self):
        alias_dic = {'bpid_tbl_alias':'bm.','src_tbl_alias':'ua.'}
        query = { 'statdate':self.stat_date,
            'convert_groupid_all':sql_const.HQL_CONVERT_GROUPID_ALL,
            'group_by_fuid_all':sql_const.HQL_GROUP_BY_FUID_ALL % {'bpid_tbl_alias':'bm.','src_tbl_alias':'br.'},
            'group_by':sql_const.HQL_GROUP_BY_ALL % {'bpid_tbl_alias':'bm.','src_tbl_alias':'ua.'},
            'group_by_no_sub_game':sql_const.HQL_GROUP_BY_NO_SUB_GAME % alias_dic,
            'group_by_include_sub_game':sql_const.HQL_GROUP_BY_INCLUDE_SUB_GAME % alias_dic,
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_int_report':sql_const.NULL_INT_REPORT
            }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.bankrupt_user_cnt_dis
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
            coalesce(count(distinct case when coalesce(bru.frupt_cnt,0) = 1 then un.fuid else null end),0) feq1,
            coalesce(count(distinct case when coalesce(bru.frupt_cnt,0) = 2 then un.fuid else null end),0) feq2,
            coalesce(count(distinct case when coalesce(bru.frupt_cnt,0) = 3 then un.fuid else null end),0) feq3,
            coalesce(count(distinct case when coalesce(bru.frupt_cnt,0) = 4 then un.fuid else null end),0) feq4,
            coalesce(count(distinct case when coalesce(bru.frupt_cnt,0) = 5 then un.fuid else null end),0) feq5,
            coalesce(count(distinct case when coalesce(bru.frupt_cnt,0) > 5 then un.fuid else null end),0) fm5
        from
            dim.reg_user_array un

        left join
            (select
                bm.fgamefsk,
                coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(br.fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(br.fchannel_code,%(null_int_group_rule)d) fchannel_code,
                fuid,
                sum(br.frupt_cnt) frupt_cnt
            from
                dim.user_bankrupt_relieve br
            join
                dim.bpid_map bm
            on
                br.fbpid = bm.fbpid
            where dt = '%(statdate)s'
            %(group_by_fuid_all)s) bru
        on un.fuid = bru.fuid
            and un.fgamefsk = bru.fgamefsk
            and un.fplatformfsk = bru.fplatformfsk
            and un.fhallfsk = bru.fhallfsk
            and un.fgame_id = bru.fgame_id
            and un.fterminaltypefsk = bru.fterminaltypefsk
            and un.fversionfsk = bru.fversionfsk
            and un.fchannel_code = bru.fchannel_code

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
            feq1,
            feq2,
            feq3,
            feq4,
            feq5,
            fm5
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
                coalesce(count(distinct case when coalesce(br.frupt_cnt,0) = 1 then ua.fuid else null end),0) feq1,
                coalesce(count(distinct case when coalesce(br.frupt_cnt,0) = 2 then ua.fuid else null end),0) feq2,
                coalesce(count(distinct case when coalesce(br.frupt_cnt,0) = 3 then ua.fuid else null end),0) feq3,
                coalesce(count(distinct case when coalesce(br.frupt_cnt,0) = 4 then ua.fuid else null end),0) feq4,
                coalesce(count(distinct case when coalesce(br.frupt_cnt,0) = 5 then ua.fuid else null end),0) feq5,
                coalesce(count(distinct case when coalesce(br.frupt_cnt,0) > 5 then ua.fuid else null end),0) fm5
            from
                dim.user_act ua
            left join
                (select fbpid,fgame_id,fuid,sum(frupt_cnt) frupt_cnt from dim.user_bankrupt_relieve
                where dt = '%(statdate)s'
                group by fbpid,fgame_id,fuid) br
            on ua.fbpid = br.fbpid
                and ua.fuid = br.fuid and ua.fgame_id = br.fgame_id
            join dim.bpid_map bm
            on ua.fbpid = bm.fbpid
            where ua.dt = '%(statdate)s'
            %(group_by)s) ot
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
a = agg_bankrupt_user_cnt_dis(statDate)
a()
