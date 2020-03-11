#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class load_user_gameparty_nomatch_cube(BaseStatModel):

    """地方棋牌牌局多维表，剔除比赛场牌局
     """
    def create_tab(self):

        hql = """
        create table if not exists veda.dfqp_user_gameparty_nomatch_cube
        (
          fhallfsk 				bigint,
          fgame_id 				bigint,
          fuid                bigint          comment '用户游戏ID',
          fparty_type         varchar(100)    comment '牌局类型',
          fpname              varchar(100)    comment '牌局一级分类名称',
          fsubname            varchar(100)    comment '牌局二级分类名称',
          fgsubname           varchar(100)    comment '牌局三级分类名称',
          fcoins_type         int             comment '结算货币类型',
          fplay_hour          int             comment '玩牌时间点',
          fparty_num          decimal(32)     comment '牌局数',
          fwin_party_num      decimal(32)     comment '赢牌局数',
          flose_party_num     decimal(32)     comment '输牌局数',
          fcharge             decimal(32,6)   comment '总台费',
          fwin_amt            decimal(32)     comment '赢得的游戏币',
          flose_amt           decimal(32)     comment '输掉的游戏币',
          fmax_win_amt        decimal(32)     comment '最大赢得的游戏币',
          fmax_lose_amt       decimal(32)     comment '最大输掉的游戏币',
          fplay_time          decimal(32)     comment '玩牌时长',
          fbankrupt_num       bigint          comment '破产次数'
        )
        partitioned by(dt date)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


    def stat(self):

        hql = """
        insert overwrite table veda.dfqp_user_gameparty_nomatch_cube
        partition( dt="%(statdate)s" )
        select tt.fhallfsk
               ,t1.fgame_id
               ,t1.fuid
               ,t1.fparty_type
               ,t1.fpname
               ,t1.fsubname
               ,t1.fgsubname
               ,t1.fcoins_type
               ,from_unixtime(floor(unix_timestamp(t1.flts_at) / 3600)* 3600,'HH') fplay_hour
               ,count(1) fparty_num
               ,sum(case when t1.fgamecoins > 0 then 1 else 0 end ) fwin_party_num
               ,sum(case when t1.fgamecoins < 0 then 1 else 0 end ) flose_party_num
               ,sum(fcharge) fcharge
               ,sum(case when t1.fgamecoins > 0 then fgamecoins else 0 end) fwin_amt
               ,sum(case when t1.fgamecoins < 0 then fgamecoins else 0 end) flose_amt
               ,max(case when t1.fgamecoins > 0 then fgamecoins else 0 end) fmax_win_amt
               ,min(case when t1.fgamecoins < 0 then fgamecoins else 0 end) fmax_lose_amt
               ,sum(case when t1.fs_timer = '1970-01-01 00:00:00' then 0
                when t1.fe_timer = '1970-01-01 00:00:00' then 0
                else unix_timestamp(t1.fe_timer)-unix_timestamp(t1.fs_timer)
                end) fplay_time
               ,sum(case when fcoins_type <> '11' and t1.fis_bankrupt = 1 then 1 else 0 end ) fbankrupt_num --只有银币的场次才是准确的
        from stage_dfqp.user_gameparty_stg t1
        join dim.bpid_map_bud tt
          on t1.fbpid = tt.fbpid
         and fgamefsk = 4132314431
       where t1.dt = '%(statdate)s'
         and concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string)) = '0_0'
        group by tt.fhallfsk
                 ,t1.fgame_id
                 ,t1.fuid
                 ,t1.fparty_type
                 ,t1.fpname
                 ,t1.fsubname
                 ,t1.fgsubname
                 ,t1.fcoins_type
                 ,from_unixtime(floor(unix_timestamp(t1.flts_at) / 3600)* 3600,'HH')
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 生成统计实例
a = load_user_gameparty_nomatch_cube(sys.argv[1:])
a()
