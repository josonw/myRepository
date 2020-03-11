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
# 可参考load_user_bycoin_balance_day.py

class load_user_bycoin_stream(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists dim.user_bycoin_stream
        (
          fbpid                varchar(50),         --BPID
          fgame_id             bigint,              --子游戏ID
          fchannel_code        bigint,              --渠道ID
          fuid                 bigint,              --用户游戏ID
          fact_id              string,              --操作编号
          fadd_amt             decimal(32),         --获得的博雅币数额
          fremove_amt          decimal(32),         --损失的博雅币数额
          flast_act_time       string,              --最后操作时间
          flast_user_bycoins   decimal(32)          --以操作编号为分组，最后的博雅币
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
        insert overwrite table dim.user_bycoin_stream
        partition( dt="%(statdate)s" )

        select
          bs.fbpid,
          coalesce(bs.fgame_id,cast (0 as bigint)) fgame_id,
          coalesce(max(ci.ftrader_id),%(null_int_report)d) fchannel_code,
          bs.fuid,
          bs.fact_id fact_id,
          sum(case when bs.fact_type = 1 then abs(bs.fact_num) else 0 end) fadd_amt,
          sum(case when bs.fact_type = 2 then abs(bs.fact_num) else 0 end) fremove_amt,
          max(bs.flast_act_time) flast_act_time,
          max(bs.flast_user_bycoins) flast_user_bycoins
        from
          (select
            fbpid,
            fgame_id,
            fchannel_code,
            fuid,
            fact_id,
            fact_type,
            fact_num,
            null flast_act_time,
            0 flast_user_bycoins
          from stage.pb_bycoins_stream_stg
          where dt = "%(statdate)s"

          union all

          select
            fbpid,
            fgame_id,
            fchannel_code,
            fuid,
            fact_id,
            -1 fact_type,
            0 fact_num,
            flts_at flast_act_time,
            fuser_bycoins_num flast_user_bycoins
          from (select
              fbpid,
              fgame_id,
              fchannel_code,
              fuid,
              fact_id,
              flts_at,
              fuser_bycoins_num,
              row_number() over(partition by fbpid,fuid,fact_id order by flts_at desc) row_num
            from stage.pb_bycoins_stream_stg
            where dt = "%(statdate)s") bs
          where row_num = 1) bs
        left join analysis.marketing_channel_pkg_info ci
        on bs.fchannel_code = ci.fid
        group by
        bs.fbpid,
        bs.fgame_id,
        bs.fuid,
        bs.fact_id
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
a = load_user_bycoin_stream(statDate, eid)
a()
