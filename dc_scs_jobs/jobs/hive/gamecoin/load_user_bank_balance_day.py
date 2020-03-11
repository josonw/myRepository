#-*- coding: UTF-8 -*-
import os
import sys
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel


class load_user_bank_balance_day(BaseStatModel):
    """
    道具结余，用户携带中间表
    """
    def create_tab(self):
        hql = """
        create table if not exists dim.user_bank_balance_day
        (
          fdate                string,              --时间
          fbpid                varchar(50),         --BPID
          fgame_id             bigint,              --子游戏ID
          fchannel_code        bigint,              --渠道ID
          fuid                 bigint,              --用户ID
          fcurrencies_type     varchar(10),         --类型id
          fbank_gamecoins_num  bigint,              --用户携带
          flast_time           string,              --最后操作时间
          flast_act_type       bigint               --最后操作类型
        )
        partitioned by(dt date)
        stored as orc
        """
        res = self.sql_exe(hql)
        return res

    def stat(self):
        # self.debug = True
        # self.sql_args.update({''})
        hql = """
        insert overwrite table dim.user_bank_balance_day
        partition( dt="%(statdate)s" )
        select  flts_at fdate,
                fbpid,
                coalesce(gs.fgame_id, %(null_int_report)d) fgame_id,
                coalesce(ci.ftrader_id, %(null_int_report)d) fchannel_code,
                fuid,
                coalesce(fcurrencies_type,'0') fcurrencies_type ,
                fbank_gamecoins_num,      --用户携带
                flts_at flast_time,       --最后操作时间
                fact_type flast_act_type  --最后操作类型
        from (
            select * from (
                select fbpid,
                       fgame_id,
                       fchannel_code,
                       fuid,
                       fcurrencies_type,
                       fact_type,
                       flts_at,
                       fbank_gamecoins_num,
                       row_number() over(partition by fbpid, fuid, fcurrencies_type order by flts_at desc, fbank_gamecoins_num desc) row_num
                  from stage.user_bank_stage t
                 where dt = '%(statdate)s'
            ) t1 where row_num = 1
        ) gs
        left join analysis.marketing_channel_pkg_info ci
        on gs.fchannel_code = ci.fid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = load_user_bank_balance_day(sys.argv[1:])
a()
