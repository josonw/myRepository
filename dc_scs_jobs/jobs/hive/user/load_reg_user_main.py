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
创建每日新增主游戏注册用户维度表
"""
class load_reg_user_main(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists dim.reg_user_main
        (
            fbpid           varchar(50),      --BPID
            fchannel_code   bigint,           --渠道ID
            fuid            bigint,           --用户游戏ID
            fsignup_at      string,           --注册时间
            fgender         tinyint,          --性别
            fversion_info   varchar(50),      --版本号
            fentrance_id    bigint,           --账号类型
            fad_code        varchar(50),      --广告激活ID
            fsource_path    varchar(100),     --来源路径
            fm_imei         varchar(100),     --设备IMEI号
            fm_dtype        varchar(100),     --终端型号
            fm_pixel        varchar(100),     --分辨率
            fm_os           varchar(100),     --系统类型
            fm_network      varchar(100),     --网络类型
            fm_operator     varchar(100),     --运营商
            fplatform_uid   varchar(50),      --付费用户的平台uid
            fpartner_info   varchar(32),      --代理商
            fpromoter       varchar(100),     -- 推广员
            fshare_key      varchar(100),     --分享新增的key
            fip             varchar(64)       --ip地址
        )
        partitioned by (dt date)
        stored as orc
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):
        hql = """
        insert overwrite table dim.reg_user_main partition (dt='%(statdate)s')
        select /*+ MAPJOIN(ci) */
            us.fbpid,
            coalesce(max(ci.ftrader_id),%(null_int_report)d) fchannel_code,
            us.fuid,
            min(us.fsignup_at) fsignup_at,
            coalesce(max(us.fgender),'%(null_str_report)s') fgender,
            coalesce(max(us.fversion_info),'%(null_str_report)s') fversion_info,
            coalesce(max(us.fentrance_id),%(null_int_report)d) fentrance_id,
            coalesce(max(us.fad_code),'%(null_str_report)s') fad_code,
            coalesce(max(us.fsource_path),'%(null_str_report)s') fsource_path,
            coalesce(max(us.fm_imei),'%(null_str_report)s') fm_imei,
            coalesce(max(us.fm_dtype),'%(null_str_report)s') fm_dtype,
            coalesce(max(us.fm_pixel),'%(null_str_report)s') fm_pixel,
            coalesce(max(us.fm_os),'%(null_str_report)s') fm_os,
            coalesce(max(us.fm_network),'%(null_str_report)s') fm_network,
            coalesce(max(us.fm_operator),'%(null_str_report)s') fm_operator,
            coalesce(max(us.fplatform_uid),'%(null_str_report)s') fplatform_uid,
            coalesce(max(us.fpartner_info),'%(null_str_report)s') fpartner_info,
            coalesce(max(us.fpromoter),'%(null_str_report)s') fpromoter,
            coalesce(max(us.fshare_key),'%(null_str_report)s') fshare_key,
            coalesce(max(us.fip),'%(null_str_report)s') fip
        from
            stage.user_signup_stg us

        left join
            analysis.marketing_channel_pkg_info ci
        on us.fchannel_code = ci.fid

        left join
            (select
                fbpid, fuid
            from dim.reg_user_main
            where dt < '%(statdate)s'
            ) um
        on us.fuid = um.fuid and us.fbpid = um.fbpid
        where us.dt = '%(statdate)s'
        and um.fuid is null
        group by us.fbpid,us.fuid
        """ % {'statdate': self.stat_date,
                'null_int_report': sql_const.NULL_INT_REPORT,
                'null_str_report': sql_const.NULL_STR_REPORT}


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
a = load_reg_user_main(statDate)
a()
