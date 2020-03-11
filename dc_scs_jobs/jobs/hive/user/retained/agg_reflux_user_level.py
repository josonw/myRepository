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


class agg_reflux_user_level(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dcnew.reflux_user_level
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
                flevel int comment '用户等级',
                fucnt bigint comment '对应等级回流用户人数'
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
        insert overwrite table dcnew.reflux_user_level
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
            uam.fgrade flevel,
            count(distinct ur.fuid) fucnt
        from
            (select
                fbpid,
                fchannel_code,
                freflux,
                fuid
            from
                dim.user_reflux
            where dt = '%(statdate)s' and fgame_id = %(null_int_report)d and freflux_type = 'cycle') ur
        left join dim.user_attribute uam
        on ur.fbpid = uam.fbpid
            and ur.fuid = uam.fuid
        join dim.bpid_map bm
        on ur.fbpid = bm.fbpid
        group by bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,bm.fterminaltypefsk,
        bm.fversionfsk,ur.fchannel_code,ur.freflux,uam.fgrade
        grouping sets (
        (bm.fgamefsk,bm.fplatformfsk,bm.fterminaltypefsk,bm.fversionfsk,ur.freflux,uam.fgrade),
        (bm.fgamefsk,bm.fplatformfsk,ur.freflux,uam.fgrade))

        union all

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
            uam.fgrade flevel,
            count(distinct ur.fuid) fucnt
        from
            (select
                fbpid,
                fchannel_code,
                freflux,
                fuid
            from
                dim.user_reflux
            where dt = '%(statdate)s' and fgame_id = %(null_int_report)d and freflux_type = 'cycle') ur
        left join dim.user_attribute uam
        on ur.fbpid = uam.fbpid
            and ur.fuid = uam.fuid
        join dim.bpid_map bm
        on ur.fbpid = bm.fbpid
        group by bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,bm.fterminaltypefsk,
        bm.fversionfsk,ur.fchannel_code,ur.freflux,uam.fgrade
        grouping sets (
        (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,bm.fterminaltypefsk,bm.fversionfsk,ur.freflux,uam.fgrade),
        (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,ur.freflux,uam.fgrade))

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
            uam.fgrade flevel,
            count(distinct ur.fuid) fucnt
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
        left join dim.user_attribute uam
        on ur.fbpid = uam.fbpid
            and ur.fuid = uam.fuid
        join dim.bpid_map bm
        on ur.fbpid = bm.fbpid
        group by bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,ur.fgame_id,bm.fterminaltypefsk,
            bm.fversionfsk,ur.fchannel_code,ur.freflux,uam.fgrade
        grouping sets (
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,ur.fgame_id,bm.fterminaltypefsk,bm.fversionfsk,ur.freflux,uam.fgrade),
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,ur.fgame_id,ur.freflux,uam.fgrade),
            (bm.fgamefsk,ur.fgame_id,ur.freflux,uam.fgrade))
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
a = agg_reflux_user_level(statDate)
a()
