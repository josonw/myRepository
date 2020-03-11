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


class load_gameparty(BaseStatModel):

#将原过程拆分为三个，分别运行，减少单个脚本资源占用，提高整体速度
#前置依赖L:load_gameparty_info
#本脚本生成 dim.user_gameparty

    """生成三个中间表
    dim.user_gameparty_stream （gameparty源表） + 7个粒度 + hallmode（ load_gameparty_info ）
    dim.gameparty_stream （桌子id为粒度）+ 7个粒度 + hallmode（ load_gameparty_stream ）
    dim.user_gameparty 以（uid，pname，subname）+ 7个粒度 + hallmode （ load_gameparty ）
     """
    def create_tab(self):

        hql = """
        create table if not exists dim.user_gameparty
        (
         fbpid varchar(50),
          fgame_id bigint,
          fchannel_code bigint,
          fuid                bigint,              --用户游戏ID
          fpname              string,              --牌局一级分类名称
          fsubname            string,              --牌局二级分类名称
          fante               string,              --底注
          fparty_num          decimal(32),         --牌局数
          fcharge             decimal(32,6),         --总台费
          fwin_amt            decimal(32),         --赢得的游戏币
          flose_amt           decimal(32),         --输掉的游戏币
          fwin_party_num      decimal(32),         --赢牌局数
          flose_party_num     decimal(32),         --输牌局数
          fplay_time          decimal(32),         --玩牌时长
          fparty_type         varchar(100),        --牌局类型
          ftrustee_num        smallint,            --使用托管次数
          fis_weedout         tinyint,             --是否淘汰（积分不够资格）
          fis_bankrupt        tinyint,             --是否破产（金币不够了）
          fis_end             tinyint,              --结束标识（0，正常结束，其他）
          ffirst_play         tinyint,
          ffirst_play_sub     tinyint,
          fgamefsk  bigint,
          fplatformfsk bigint,
          fhallfsk bigint,
          fterminaltypefsk bigint,
          fversionfsk bigint,
          hallmode smallint
        )
        partitioned by(dt date)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


    def stat(self):

        hql ="""set hive.groupby.skewindata=true; """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql ="""
        insert overwrite table dim.user_gameparty
        partition( dt="%(statdate)s" )
        select
            fbpid,
            fgame_id,
            fchannel_code,
            fuid,
            fpname,
            fsubname,
            fblind_1,
            count(1) fparty_num,
            sum(fcharge) fcharge,
            sum( case when ug.fgamecoins > 0 then fgamecoins else 0 end) fwin_amt,
            sum( case when ug.fgamecoins < 0 then abs(fgamecoins) else 0 end) flose_amt,
            sum( case when ug.fgamecoins > 0 then 1 else 0 end ) fwin_party_num,
            sum( case when ug.fgamecoins < 0 then 1 else 0 end ) flose_party_num,
            sum(case when ug.fs_timer = '1970-01-01 00:00:00' then 0
                when ug.fe_timer = '1970-01-01 00:00:00' then 0
                when unix_timestamp(fe_timer)-unix_timestamp(fs_timer) >=86400 then 0
                else unix_timestamp(ug.fe_timer)-unix_timestamp(ug.fs_timer)
                end) fplay_time,
            fparty_type,
            max(ftrustee_num) ftrustee_num,
            max(fis_weedout) fis_weedout,
            max(fis_bankrupt) fis_bankrupt,
            max(fis_end) fis_end,
            max(ffirst_play) ffirst_play,
            max(ffirst_play_sub) ffirst_play_sub,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fterminaltypefsk,
            fversionfsk,
            hallmode
        from dim.user_gameparty_stream ug
       where dt = '%(statdate)s'
        group by
            fbpid,
            fgame_id,
            fchannel_code,
            fuid,
            fpname,
            fsubname,
            fblind_1,
            fparty_type,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fterminaltypefsk,
            fversionfsk,
            hallmode
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = load_gameparty(sys.argv[1:])
a()
