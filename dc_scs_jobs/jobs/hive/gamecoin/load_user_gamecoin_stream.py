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

# 新后台迁移的任务，中间表设计不合理，后期要废弃
# load_user_gamecoin_balance_day

class load_user_gamecoin_stream(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists dim.user_gamecoin_stream
        (
          fbpid                varchar(50),         --BPID
          fgame_id             bigint,              --子游戏ID
          fchannel_code        bigint,              --渠道ID
          fuid                 bigint,              --用户游戏ID
          fact_id              string,              --操作编号
          fadd_amt             decimal(32),         --获得的游戏币数额
          fremove_amt          decimal(32),         --损失的游戏币数额
          flast_act_time       string,              --最后操作时间
          flast_user_gamecoins decimal(32)          --以操作编号为分组，最后的游戏币
        )
        partitioned by(dt date)
        stored as orc
        """

        res = self.hq.exe_sql(hql)
        return res

    def stat(self):
        query = {'statdate':self.stat_date,
          'null_int_report':sql_const.NULL_INT_REPORT}

        hql = """
        insert overwrite table dim.user_gamecoin_stream
        partition( dt="%(statdate)s" )

        select
          gs.fbpid,
          coalesce(gs.fgame_id,cast (0 as bigint)) fgame_id,
          coalesce(max(ci.ftrader_id),%(null_int_report)d) fchannel_code,
          gs.fuid,
          gs.act_id fact_id,
          sum(case when gs.act_type = 1 then abs(gs.act_num) else 0 end) fadd_amt,
          sum(case when gs.act_type = 2 then abs(gs.act_num) else 0 end) fremove_amt,
          max(gs.flast_act_time) flast_act_time,
          max(gs.flast_user_gamecoins) flast_user_gamecoins
        from
          (select
            fbpid,
            fgame_id,
            fchannel_code,
            fuid,
            act_id,
            act_type,
            act_num,
            null flast_act_time,
            0 flast_user_gamecoins
          from stage.pb_gamecoins_stream_stg
          where dt = "%(statdate)s"

          union all

          select
            fbpid,
            fgame_id,
            fchannel_code,
            fuid,
            act_id,
            -1 act_type,
            0 act_num,
            lts_at flast_act_time,
            user_gamecoins_num flast_user_gamecoins
          from (select
              fbpid,
              fgame_id,
              fchannel_code,
              fuid,
              act_id,
              lts_at,
              user_gamecoins_num,
              row_number() over(partition by fbpid,fuid,act_id order by lts_at desc) row_num
            from stage.pb_gamecoins_stream_stg
            where dt = "%(statdate)s") gs
          where row_num = 1) gs
        left join analysis.marketing_channel_pkg_info ci
        on gs.fchannel_code = ci.fid
        group by
        gs.fbpid,
        gs.fgame_id,
        gs.fuid,
        gs.act_id
        """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


#愉快的统计要开始啦
global statDate
eid = 0
if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
elif len(sys.argv) >= 3:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]
    eid = int(sys.argv[2])
else :
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = load_user_gamecoin_stream(statDate, eid)
a()
