#-*- coding: UTF-8 -*-
import os
import sys
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel


class load_user_gamecoin_balance_day(BaseStatModel):
    """
    游戏币结余，用户携带中间表
    """
    def create_tab(self):
        hql = """
        create table if not exists dim.user_gamecoin_balance_day
        (
          fdate                string,              --时间
          fbpid                varchar(50),         --BPID
          fgame_id             bigint,              --子游戏ID
          fchannel_code        bigint,              --渠道ID
          fuid                 bigint,              --用户游戏ID
          fgamecoins           bigint,              --用户游戏币携带
          flast_time           string,              --最后操作时间
          flast_act_type       bigint,              --最后操作类型
          flast_act_id         bigint              --最后操作编号
        )
        partitioned by(dt date)
        stored as orc
        """
        res = self.sql_exe(hql)
        return res

    def stat(self):
        # self.debug = True
        # self.sql_args.update({''})
        hql  = """
        insert overwrite table dim.user_gamecoin_balance_day
        partition( dt="%(statdate)s" )
        select  lts_at fdate,
                fbpid,
                coalesce(gs.fgame_id,cast (0 as bigint)) fgame_id,
                coalesce(ci.ftrader_id, %(null_int_report)d) fchannel_code,
                fuid,
                user_gamecoins_num flast_user_gamecoins,
                lts_at,
                act_type,
                act_id
        from (
            select * from (
                select
                  fbpid,
                  fgame_id,
                  fchannel_code,
                  fuid,
                  act_type,
                  act_id,
                  lts_at,
                  user_gamecoins_num,
                  row_number() over(partition by fbpid, fuid order by lts_at desc, nvl(fseq_no,0) desc, user_gamecoins_num desc) row_num
                from stage.pb_gamecoins_stream_stg
                where dt = "%(statdate)s"
            ) t1 where row_num = 1
        ) gs
        left join analysis.marketing_channel_pkg_info ci
        on gs.fchannel_code = ci.fid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

#生成统计实例
a = load_user_gamecoin_balance_day(sys.argv[1:])
a()
