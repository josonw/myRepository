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


class agg_act_user_lang_dis(BaseStat):

    def create_tab(self):

        hql = """create table if not exists dcnew.act_user_lang_dis
            (
                fdate date comment '数据日期',
                fgamefsk bigint comment '游戏ID',
                fplatformfsk bigint comment '平台ID',
                fhallfsk bigint comment '大厅ID',
                fsubgamefsk bigint comment '子游戏ID',
                fterminaltypefsk bigint comment '终端类型ID',
                fversionfsk bigint comment '应用包版本ID',
                fchannelcode bigint comment '渠道ID',
                flang varchar(64) comment '用户语言',
                fucnt bigint comment '对应语言活跃用户人数'
            )
            partitioned by (dt date)
        """

        res = self.hq.exe_sql(hql)


    def stat(self):
        query = { 'statdate':self.stat_date,
            'null_str_group_rule':sql_const.NULL_STR_GROUP_RULE,
            'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
            'null_int_report':sql_const.NULL_INT_REPORT,
            'null_str_report':sql_const.NULL_STR_REPORT
            }

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
	set hive.auto.convert.join=false;
        insert overwrite table dcnew.act_user_lang_dis
        partition(dt = '%(statdate)s' )

        select
            '%(statdate)s' fdate,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fgame_id fsubgamefsk,
            fterminaltypefsk,
            fversionfsk,
            fchannel_code fchannelcode,
            flang,
            fucnt
        from

            (select
                bm.fgamefsk,
                coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(ua.fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(ua.fchannel_code,%(null_int_group_rule)d) fchannel_code,
                coalesce(uam.flang,'%(null_str_report)s') flang,
                count(distinct ua.fuid) fucnt
            from
                dim.user_act ua
            left join
                dim.user_act_main uam
            on ua.fbpid = uam.fbpid
                and ua.fuid = uam.fuid
            join dim.bpid_map bm
            on ua.fbpid = bm.fbpid
            where ua.dt = '%(statdate)s' and uam.dt = '%(statdate)s'
            group by bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,ua.fgame_id,bm.fterminaltypefsk,
            bm.fversionfsk,ua.fchannel_code,uam.flang
            grouping sets (
                (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,ua.fgame_id,bm.fterminaltypefsk,bm.fversionfsk,uam.flang),
                (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,ua.fgame_id,uam.flang),
                (bm.fgamefsk,ua.fgame_id,uam.flang),
                (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,bm.fterminaltypefsk,bm.fversionfsk,uam.flang),
                (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,uam.flang),
                (bm.fgamefsk,bm.fplatformfsk,bm.fterminaltypefsk,bm.fversionfsk,uam.flang),
                (bm.fgamefsk,bm.fplatformfsk,uam.flang))
            ) src

        where fgame_id <> %(null_int_report)d;
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
a = agg_act_user_lang_dis(statDate)
a()
