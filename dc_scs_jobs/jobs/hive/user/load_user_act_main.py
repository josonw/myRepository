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

class load_user_act_main(BaseStat):
    """
    创建主游戏每日活跃用户维度表，不带子游戏，相当于活跃用户的属性表，可用于多种场合
    """
    def create_tab(self):
        hql = """
        create table if not exists dim.user_act_main
        (
            fbpid                varchar(50),      --BPID
            fchannel_code        bigint,           --渠道ID
            fuid                 bigint,           --用户游戏ID
            flast_user_gamecoins bigint,           --最后携带游戏币
            fis_change_gamecoins tinyint,          --金流是否发生变化
            fearly_login_at      string,           --当天首次登录时间
            flast_login_at       string,           --当天最后登录时间
            flogin_cnt           int,              --当天登录次数
            fparty_num           int,              --当天牌局数
            flast_level          int,              --当天用户最后等级
            fentrance_id         bigint,           --账号类型
            fmax_version_info    varchar(50),      --当天最大版本号
            flang                varchar(64),      --语言
            fsource_path         varchar(100),     --来源路径
            ffirst_user_gamecoins bigint           --首次携带游戏币
        )
        partitioned by (dt date)
        stored as orc
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):
        query = {'statdate':self.stat_date,
                'null_str_report':sql_const.NULL_STR_REPORT,
                'null_int_report':sql_const.NULL_INT_REPORT}

        self.hql_dict.update(query)

        hql = """
        drop table if exists work.user_active_main_login_%(num_begin)s;
        create table work.user_active_main_login_%(num_begin)s
        as
        select
            src.fbpid,
            coalesce(max(ci.ftrader_id),%(null_int_report)d) fchannel_code,
            src.fuid,
            max(ffirst_user_gamecoins) ffirst_user_gamecoins,
            max(fearly_login_at) fearly_login_at,
            max(flast_login_at) flast_login_at,
            sum(flogin_cnt) flogin_cnt,
            max(flast_level) flast_level,
            max(fentrance_id) fentrance_id,
            max(fmax_version_info) fmax_version_info,
            max(flang) flang,
            max(fsource_path) fsource_path
        from
            (select
                fbpid,
                fchannel_code,
                fuid,
                user_gamecoins ffirst_user_gamecoins,
                null fearly_login_at,
                flogin_at flast_login_at,
                0 flogin_cnt,
                0 flast_level,
                0 fentrance_id,
                null fmax_version_info,
                null flang,
                null fsource_path
            from (select fbpid,
                        fchannel_code,
                        fuid,
                        user_gamecoins,
                        flevel,
                        flogin_at,
                        row_number() over(partition by fbpid,fuid order by flogin_at ) row_num
                        from dim.user_login_additional
                        where dt='%(statdate)s'
                ) ull
            where row_num = 1

            union all
            select
                fbpid,
                fchannel_code,
                fuid,
                0 ffirst_user_gamecoins,
                min(flogin_at) fearly_login_at,
                max(flogin_at) flast_login_at,
                count(1) flogin_cnt,
                max(flevel) flast_level,
                max(fentrance_id) fentrance_id,
                max(fversion_info) fmax_version_info,
                max(flang) flang,
                max(fsource_path) fsource_path
            from
                dim.user_login_additional
            where dt='%(statdate)s'
            group by fbpid, fchannel_code, fuid
            ) src
        left join analysis.marketing_channel_pkg_info ci
            on src.fchannel_code = ci.fid
            group by src.fbpid,src.fuid;
        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        res = self.hq.exe_sql(
            "set mapreduce.input.fileinputformat.split.maxsize=64000000;")
        if res != 0: return res

        hql = """
        insert overwrite table dim.user_act_main partition ( dt='%(statdate)s' )
        select
            fbpid,
            max(fchannel_code) fchannel_code,
            fuid,
            max(flast_user_gamecoins) flast_user_gamecoins,
            max(fis_change_gamecoins) fis_change_gamecoins,
            max(fearly_login_at) fearly_login_at,
            max(flast_login_at) flast_login_at,
            max(flogin_cnt) flogin_cnt,
            max(fparty_num) fparty_num,
            max(flast_level) flast_level,
            max(fentrance_id) fentrance_id,
            max(fmax_version_info) fmax_version_info,
            max(flang) flang,
            max(fsource_path) fsource_path,
            max(ffirst_user_gamecoins) ffirst_user_gamecoins
        from (
            select
                fbpid,
                fchannel_code,
                fuid,
                0 flast_user_gamecoins,
                0 fis_change_gamecoins,
                fearly_login_at,
                flast_login_at,
                flogin_cnt,
                0 fparty_num,
                flast_level,
                fentrance_id,
                fmax_version_info,
                flang,
                fsource_path,
                ffirst_user_gamecoins
            from work.user_active_main_login_%(num_begin)s

            union all

            select
                fbpid,
                fchannel_code,
                fuid,
                0 flast_user_gamecoins,
                0 fis_change_gamecoins,
                null fearly_login_at,
                null flast_login_at,
                0 flogin_cnt,
                fparty_num fparty_num,
                0 flast_level,
                0 fentrance_id,
                null fmax_version_info,
                null flang,
                null fsource_path,
                null ffirst_user_gamecoins
            from dim.user_gameparty
            where dt='%(statdate)s'

            union all

            select
                fbpid,
                fchannel_code,
                fuid,
                0 flast_user_gamecoins,
                1 fis_change_gamecoins,
                null fearly_login_at,
                null flast_login_at,
                0 flogin_cnt,
                0 fparty_num,
                0 flast_level,
                0 fentrance_id,
                null fmax_version_info,
                null flang,
                null fsource_path,
                null ffirst_user_gamecoins
            from dim.user_gamecoin_stream_day
            where dt='%(statdate)s' and fact_id not in ('8', '169', '960')
              and (cast(fact_id as int) is null or fact_id >= 0)

            union all

            select
                fbpid,
                %(null_int_report)d fchannel_code,
                fuid,
                0 flast_user_gamecoins,
                0 fis_change_gamecoins,
                null fearly_login_at,
                null flast_login_at,
                0 flogin_cnt,
                count(1) fparty_num,
                0 flast_level,
                0 fentrance_id,
                null fmax_version_info,
                null flang,
                null fsource_path,
                null ffirst_user_gamecoins
            from stage.finished_gameparty_uid_mid
            where dt='%(statdate)s'
            group by fbpid, fuid
        ) t2 group by fbpid, fuid
        ;
        drop table work.user_active_main_login_%(num_begin)s;
        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        if res != 0: return res

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
a = load_user_act_main(statDate)
a()
