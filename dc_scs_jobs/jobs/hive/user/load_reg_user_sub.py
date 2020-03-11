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

"""
创建每日进入子游戏用户维度表
"""
class load_reg_user_sub(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists dim.reg_user_sub
        (
            fbpid            varchar(50),  --BPID
            fgame_id         int,          --子游戏ID
            fchannel_code    bigint,       --渠道ID
            fuid             bigint,       --用户游戏ID
            fis_first        tinyint,      --是否为首次进入子游戏
            ffirst_at        string,       --首次进入子游戏时间
            fenter_cnt       int,          --当天进入子游戏次数
            fentrance_id     bigint,       --账号类型
            fversion_info    varchar(50),  --版本号
            fad_code         varchar(50),  --广告激活ID
            fsource_path     varchar(100), --来源路径
            fm_imei          varchar(100), --设备IMEI号
            fip              varchar(64)   --ip地址
        )
        partitioned by (dt date)
        stored as orc
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):
        query = { 'statdate':self.stat_date,
        'null_int_report':sql_const.NULL_INT_REPORT,
        'null_str_report':sql_const.NULL_STR_REPORT}


        hql = """
        insert overwrite table dim.reg_user_sub partition ( dt='%(statdate)s' )

        select fbpid, fgame_id, max(fchannel_code) fchannel_code, fuid,
               max(fis_first) fis_first, max(ffirst_at) ffirst_at, max(fenter_cnt) fenter_cnt,
               max(fentrance_id) fentrance_id, max(fversion_info) fversion_info,
               max(fad_code) fad_code, max(fsource_path) fsource_path, max(fm_imei) fm_imei, max(fip) fip
        from (
            select
            ue.fbpid,
            coalesce(ue.fgame_id,cast (0 as bigint)) fgame_id,
            coalesce(max(ci.ftrader_id),%(null_int_report)d) fchannel_code,
            ue.fuid,
            max(ue.fis_first) fis_first,
            max(case when
                    ue.fis_first = 1 then ue.flts_at else null
                end) ffirst_at,
            count(ue.fuid) fenter_cnt,
            coalesce(max(ue.fentrance_id),%(null_int_report)d) fentrance_id,
            coalesce(max(ue.fversion_info),'%(null_str_report)s') fversion_info,
            coalesce(max(ue.fad_code),'%(null_str_report)s') fad_code,
            coalesce(max(ue.fsource_path),'%(null_str_report)s') fsource_path,
            coalesce(max(ue.fm_imei),'%(null_str_report)s') fm_imei,
            coalesce(max(ue.fip),'%(null_str_report)s') fip
            from
                stage.user_enter_stg ue
            left join
                analysis.marketing_channel_pkg_info ci
            on ue.fchannel_code = ci.fid
            where ue.dt = '%(statdate)s'
            group by fbpid,ue.fgame_id,fuid

        union all

            select /*+ MAPJOIN(ci) */ ug.fbpid, coalesce(ug.fgame_id,cast (0 as bigint)) fgame_id,
                coalesce(max(ci.ftrader_id),%(null_int_report)d) fchannel_code,
                ug.fuid, null fis_first, null ffirst_at, null fenter_cnt, null fentrance_id, null fversion_info,
                null fad_code, null fsource_path, null fm_imei, null fip
            from stage.user_gameparty_stg ug
            left join
            analysis.marketing_channel_pkg_info ci
            on ug.fchannel_code = ci.fid
            where ug.dt = '%(statdate)s'
            group by fbpid,ug.fgame_id,fuid


        union all

            select /*+ MAPJOIN(ci) */ ps.fbpid, coalesce(ps.fgame_id,cast (0 as bigint)) fgame_id,
                coalesce(max(ci.ftrader_id),%(null_int_report)d) fchannel_code,
                ps.fuid, null fis_first, null ffirst_at, null fenter_cnt, null fentrance_id, null fversion_info,
                null fad_code, null fsource_path,null fm_imei, null fip
            from stage.pb_gamecoins_stream_stg ps
            left join
            analysis.marketing_channel_pkg_info ci
            on ps.fchannel_code = ci.fid
            where ps.dt = '%(statdate)s'
            group by fbpid,ps.fgame_id,fuid
            ) a

        group by fbpid,fgame_id,fuid
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
a = load_reg_user_sub(statDate)
a()
