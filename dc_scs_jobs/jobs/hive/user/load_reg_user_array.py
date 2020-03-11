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


class load_reg_user_array(BaseStat):
    """
    创建每日新增子游戏注册用户维度表
    """
    def create_tab(self):
        hql = """
        create table if not exists dim.reg_user_array
        (
            fgamefsk         bigint,           --游戏ID
            fplatformfsk     bigint,           --平台ID
            fhallfsk         bigint,           --大厅ID
            fterminaltypefsk bigint,           --终端ID
            fversionfsk      bigint,           --版本ID
            fchannel_code    bigint,           --渠道ID
            fgame_id         bigint,           --子游戏ID
            fuid             bigint,           --用户游戏ID
            fsignup_at       string,           --注册时间
            fgender          tinyint,          --性别
            fversion_info    varchar(50),      --版本号
            fentrance_id     bigint,           --账号类型
            fad_code         varchar(50),      --广告激活ID
            fsource_path     varchar(100),     --来源路径
            fm_imei          varchar(100)      --设备IMEI号
        )
        partitioned by (dt date)
        stored as orc
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res
        hql = """
        insert overwrite table dim.reg_user_array partition (dt='%(statdate)s')

        select
            coalesce(bm.fgamefsk,%(null_int_group_rule)d) fgamefsk,
            coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
            coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(us.fchannel_code,%(null_int_group_rule)d) fchannel_code,
            coalesce(us.fgame_id,%(null_int_group_rule)d) fgame_id,
            us.fuid,
            max(us.ffirst_at) fsignup_at,
            max(unm.fgender) fgender,
            max(uam.fmax_version_info) fversion_info,
            max(uam.fentrance_id) fentrance_id,
            max(unm.fad_code) fad_code,
            max(uam.fsource_path) fsource_path,
            max(unm.fm_imei) fm_imei
        from
            dim.reg_user_sub us
        join
            dim.bpid_map bm
        on us.fbpid = bm.fbpid
        left join
            dim.user_act_main uam
        on us.fbpid = uam.fbpid and us.fuid = uam.fuid
            and uam.dt = '%(statdate)s'
        left join
            (select
                fbpid,fuid,fgender,fad_code,fm_imei
            from dim.reg_user_main_additional
            where dt <= '%(statdate)s'
            ) unm
        on us.fuid = unm.fuid and us.fbpid = unm.fbpid
        where us.dt = '%(statdate)s'
            and us.fis_first = 1
        group by bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,us.fgame_id,bm.fterminaltypefsk,bm.fversionfsk,us.fchannel_code,us.fuid
        grouping sets (
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,us.fgame_id,bm.fterminaltypefsk,bm.fversionfsk,us.fuid),
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,us.fgame_id,us.fuid),
            (bm.fgamefsk,us.fgame_id,us.fuid))

        union all

        select /*+ MAPJOIN(bm) */
            coalesce(bm.fgamefsk,%(null_int_group_rule)d) fgamefsk,
            coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
            coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(unm.fchannel_code,%(null_int_group_rule)d) fchannel_code,
            %(null_int_group_rule)d fgame_id,
            unm.fuid,
            max(unm.fsignup_at) fsignup_at,
            max(unm.fgender) fgender,
            max(unm.fversion_info) fversion_info,
            max(unm.fentrance_id) fentrance_id,
            max(unm.fad_code) fad_code,
            max(unm.fsource_path) fsource_path,
            max(unm.fm_imei) fm_imei
        from
            dim.reg_user_main_additional unm
        join
            dim.bpid_map bm
        on unm.fbpid = bm.fbpid
        where unm.dt = '%(statdate)s' and bm.hallmode = 1
        group by bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,bm.fterminaltypefsk,bm.fversionfsk,
        unm.fchannel_code,unm.fuid
        grouping sets (
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,bm.fterminaltypefsk,bm.fversionfsk,unm.fuid),
            (bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,unm.fuid),
            (bm.fgamefsk,bm.fplatformfsk,unm.fuid))

        union all

        select /*+ MAPJOIN(bm) */
            coalesce(bm.fgamefsk,%(null_int_group_rule)d) fgamefsk,
            coalesce(bm.fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(bm.fhallfsk,%(null_int_group_rule)d) fhallfsk,
            coalesce(bm.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(bm.fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(unm.fchannel_code,%(null_int_group_rule)d) fchannel_code,
            %(null_int_group_rule)d fgame_id,
            unm.fuid,
            max(unm.fsignup_at) fsignup_at,
            max(unm.fgender) fgender,
            max(unm.fversion_info) fversion_info,
            max(unm.fentrance_id) fentrance_id,
            max(unm.fad_code) fad_code,
            max(unm.fsource_path) fsource_path,
            max(unm.fm_imei) fm_imei
        from
            dim.reg_user_main_additional unm
        join
            dim.bpid_map bm
        on unm.fbpid = bm.fbpid
        where unm.dt = '%(statdate)s' and bm.hallmode = 0
        group by bm.fgamefsk,bm.fplatformfsk,bm.fhallfsk,bm.fterminaltypefsk,bm.fversionfsk,
        unm.fchannel_code,unm.fuid
        grouping sets (
            (bm.fgamefsk,bm.fplatformfsk,bm.fterminaltypefsk,bm.fversionfsk,unm.fuid),
            (bm.fgamefsk,bm.fplatformfsk,unm.fuid)
        )

        """ % { 'statdate':self.stat_date,
        'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE,
                'null_int_group_rule':sql_const.NULL_INT_GROUP_RULE
        }

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
a = load_reg_user_array(statDate)
a()
