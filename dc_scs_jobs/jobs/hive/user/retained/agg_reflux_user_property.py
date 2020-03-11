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


class agg_reflux_user_property(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dcnew.reflux_user_property
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                freflux int comment '回流天数，如7表示7日回流',
                feq0 bigint comment '资产为0的回流用户人数',
                fm1lq1k bigint comment '资产大于1且不超过1千的回流用户人数',
                fm1klq5k bigint comment '资产大于1千且不超过5千的回流用户人数',
                fm5Klq10k bigint  comment '资产大于5千且不超过1万的回流用户人数',
                fm10Klq50k bigint  comment '资产大于1万且不超过5万的回流用户人数',
                fm50Klq100k bigint  comment '资产大于5万且不超过10万的回流用户人数',
                fm100Klq500k bigint  comment '资产大于10万且不超过50万的回流用户人数',
                fm500Klq1m bigint  comment '资产大于50万且不超过1百万的回流用户人数',
                fm1mlq5m bigint  comment '资产大于1百万千且不超过5百万的回流用户人数',
                fm5mlq10m bigint  comment '资产大于5百万且不超过1千万的回流用户人数',
                fm10mlq50m bigint  comment '资产大于1千万且不超过5千万的回流用户人数',
                fm50mlq100m bigint  comment '资产大于5千万且不超过1亿的回流用户人数',
                fm100mlq1b bigint  comment '资产大于1亿且不超过10亿的回流用户人数',
                fm1b bigint  comment '资产大于10亿的回流用户人数'
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)


    def stat(self):
        query = { 'statdate':self.stat_date,
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_int_report':sql_const.NULL_INT_REPORT
            }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.reflux_user_property
        partition(dt = '%(statdate)s' )

        select
            '%(statdate)s' fdate,
            bm.fgamefsk,
            coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
            %(null_int_group_rule)d fsubgamefsk,
            coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(ur.fchannel_code,%(null_int_group_rule)d) fchannelcode,
            ur.freflux,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) = 0 then ur.fuid else null end),0) feq0,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 0 and coalesce(uam.flast_user_gamecoins,0) <= 1000 then ur.fuid else null end),0) fm1lq1k,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 1000 and coalesce(uam.flast_user_gamecoins,0) <= 5000 then ur.fuid else null end),0) fm1klq5k,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 5000 and coalesce(uam.flast_user_gamecoins,0) <= 10000 then ur.fuid else null end),0) fm5Klq10k,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 10000 and coalesce(uam.flast_user_gamecoins,0) <= 50000 then ur.fuid else null end),0) fm10Klq50k,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 50000 and coalesce(uam.flast_user_gamecoins,0) <= 100000 then ur.fuid else null end),0) fm50Klq100k,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 100000 and coalesce(uam.flast_user_gamecoins,0) <= 500000 then ur.fuid else null end),0) fm100Klq500k,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 500000 and coalesce(uam.flast_user_gamecoins,0) <= 1000000 then ur.fuid else null end),0) fm500Klq1m,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 1000000 and coalesce(uam.flast_user_gamecoins,0) <= 5000000 then ur.fuid else null end),0) fm1mlq5m,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 5000000 and coalesce(uam.flast_user_gamecoins,0) <= 10000000 then ur.fuid else null end),0) fm5mlq10m,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 10000000 and coalesce(uam.flast_user_gamecoins,0) <= 50000000 then ur.fuid else null end),0) fm10mlq50m,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 50000000 and coalesce(uam.flast_user_gamecoins,0) <= 100000000 then ur.fuid else null end),0) fm50mlq100m,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 100000000 and coalesce(uam.flast_user_gamecoins,0) <= 1000000000 then ur.fuid else null end),0) fm100mlq1b,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 1000000000 then ur.fuid else null end),0) fm1b
        from
            (select
                fbpid,
                fchannel_code,
                freflux,
                fuid
            from
                dim.user_reflux
            where dt = '%(statdate)s' and fgame_id = %(null_int_report)d and freflux_type = 'cycle') ur
        left join
            (select
                fbpid,fuid,flast_user_gamecoins
            from dim.user_act_main
            where dt = '%(statdate)s') uam
        on ur.fbpid = uam.fbpid
            and ur.fuid = uam.fuid
        join dim.bpid_map bm
        on ur.fbpid = bm.fbpid
        group by bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,bm.fterminaltypefsk,
        bm.fversionfsk,ur.fchannel_code,ur.freflux
        grouping sets (
        (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,bm.fterminaltypefsk,bm.fversionfsk,ur.freflux),
        (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,ur.freflux),
        (bm.fgamefsk,bm.fplatformfsk,bm.fterminaltypefsk,bm.fversionfsk,ur.freflux),
        (bm.fgamefsk,bm.fplatformfsk,ur.freflux))

        union all

        select
            '%(statdate)s' fdate,
            bm.fgamefsk,
            coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
            ur.fgame_id fsubgamefsk,
            coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(ur.fchannel_code,%(null_int_group_rule)d) fchannelcode,
            ur.freflux,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) = 0 then ur.fuid else null end),0) feq0,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 0 and coalesce(uam.flast_user_gamecoins,0) <= 1000 then ur.fuid else null end),0) fm1lq1k,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 1000 and coalesce(uam.flast_user_gamecoins,0) <= 5000 then ur.fuid else null end),0) fm1klq5k,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 5000 and coalesce(uam.flast_user_gamecoins,0) <= 10000 then ur.fuid else null end),0) fm5Klq10k,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 10000 and coalesce(uam.flast_user_gamecoins,0) <= 50000 then ur.fuid else null end),0) fm10Klq50k,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 50000 and coalesce(uam.flast_user_gamecoins,0) <= 100000 then ur.fuid else null end),0) fm50Klq100k,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 100000 and coalesce(uam.flast_user_gamecoins,0) <= 500000 then ur.fuid else null end),0) fm100Klq500k,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 500000 and coalesce(uam.flast_user_gamecoins,0) <= 1000000 then ur.fuid else null end),0) fm500Klq1m,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 1000000 and coalesce(uam.flast_user_gamecoins,0) <= 5000000 then ur.fuid else null end),0) fm1mlq5m,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 5000000 and coalesce(uam.flast_user_gamecoins,0) <= 10000000 then ur.fuid else null end),0) fm5mlq10m,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 10000000 and coalesce(uam.flast_user_gamecoins,0) <= 50000000 then ur.fuid else null end),0) fm10mlq50m,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 50000000 and coalesce(uam.flast_user_gamecoins,0) <= 100000000 then ur.fuid else null end),0) fm50mlq100m,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 100000000 and coalesce(uam.flast_user_gamecoins,0) <= 1000000000 then ur.fuid else null end),0) fm100mlq1b,
            coalesce(count(distinct case when coalesce(uam.flast_user_gamecoins,0) > 1000000000 then ur.fuid else null end),0) fm1b
        from
            (select
                fbpid,
                fgame_id,
                fchannel_code,
                freflux,
                fuid
            from
                dim.user_reflux
            where dt = '%(statdate)s' and fgame_id <> %(null_int_report)d and freflux_type = 'cycle') ur
        left join
            (select
                fbpid,fuid,flast_user_gamecoins
            from dim.user_act_main
            where dt = '%(statdate)s') uam
        on ur.fbpid = uam.fbpid
            and ur.fuid = uam.fuid
        join dim.bpid_map bm
        on ur.fbpid = bm.fbpid
        group by bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,ur.fgame_id,bm.fterminaltypefsk,
            bm.fversionfsk,ur.fchannel_code,ur.freflux
        grouping sets (
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,ur.fgame_id,bm.fterminaltypefsk,bm.fversionfsk,ur.freflux),
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,ur.fgame_id,ur.freflux),
            (bm.fgamefsk,ur.fgame_id,ur.freflux))
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
a = agg_reflux_user_property(statDate)
a()
