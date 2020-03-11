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
创建每日活跃用户维度表
"""
class load_user_act(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists dim.user_act
        (
            fbpid                varchar(50),       --BPID
            fgame_id             bigint,            --子游戏ID
            fchannel_code        bigint,            --渠道ID
            fuid                 bigint,            --用户ID
            flogin_cnt           int,               --登录次数
            fparty_num           int,               --牌局数
            fis_change_gamecoins tinyint            --金流是否发生变化
        )
        partitioned by (dt date)
        stored as orc
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):
        hql = """
        insert overwrite table dim.user_act partition ( dt='%(statdate)s' )
        select
            fbpid,
            fgame_id,
            max(fchannel_code) fchannel_code,
            fuid,
            sum(flogin_cnt) flogin_cnt,
            sum(fparty_num) fparty_num,
            max(fis_change_gamecoins) fis_change_gamecoins
        from
        (
            select
                fbpid,
                %(null_int_report)d fgame_id,
                fchannel_code,
                fuid,
                flogin_cnt,
                fparty_num,
                fis_change_gamecoins
            from dim.user_act_main
            where dt='%(statdate)s'

            union all
--子游戏登陆中的子游戏id+登录次数
            select
                us.fbpid,
                us.fgame_id,
                us.fchannel_code,
                us.fuid,
                us.fenter_cnt flogin_cnt,
                0 fparty_num,
                0 fis_change_gamecoins
            from dim.reg_user_sub us
            where us.dt='%(statdate)s' and us.fgame_id is not null

            union all
--子游戏牌局中的子游戏id+牌局数
            select
                gi.fbpid,
                gi.fgame_id,
                gi.fchannel_code,
                gi.fuid,
                0 flogin_cnt,
                coalesce(gi.fparty_num,0) fparty_num,
                0 fis_change_gamecoins
            from dim.user_gameparty gi
            where gi.dt='%(statdate)s' and gi.fgame_id is not null

            union all
--子游戏金流中的子游戏id+金流变化
            select
                gi.fbpid,
                gi.fgame_id,
                gi.fchannel_code,
                gi.fuid,
                0 flogin_cnt,
                0 fparty_num,
                1 fis_change_gamecoins
            from dim.user_gamecoin_stream_day gi
            where gi.dt='%(statdate)s' and gi.fgame_id is not null
        ) src
        group by fbpid, fgame_id, fuid
        """ % {'statdate':self.stat_date,'null_str_report':sql_const.NULL_STR_REPORT,
        'null_int_report':sql_const.NULL_INT_REPORT}

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
a = load_user_act(statDate)
a()
