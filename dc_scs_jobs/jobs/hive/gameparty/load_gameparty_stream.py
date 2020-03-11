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


class load_gameparty_stream(BaseStatModel):

#将原过程拆分为三个，分别运行，减少单个脚本资源占用，提高整体速度
#前置依赖L:load_gameparty_info
#本脚本生成 dim.gameparty_stream

    """生成三个中间表
    dim.user_gameparty_stream （gameparty源表） + 7个粒度 + hallmode（ load_gameparty_info ）
    dim.gameparty_stream （桌子id为粒度）+ 7个粒度 + hallmode（ load_gameparty_stream ）
    dim.user_gameparty 以（uid，pname，subname）+ 7个粒度 + hallmode （ load_gameparty ）
     """
    def create_tab(self):
        hql = """
        create table if not exists dim.gameparty_stream
        (
          fbpid varchar(50),
          fgame_id bigint,
          fchannel_code bigint,
          ftbl_id             varchar(64),         --桌子编号
          finning_id          varchar(64),         --牌局编号
          fpname              string,              --牌局一级分类名称
          fsubname            string,              --牌局二级分类名称
          fparty_type         varchar(100),        --牌局类型
          fpalyer_cnt         smallint,            --参与牌局人数，包括自己
          fante               string,              --底注
          fparty_num          bigint,              --玩牌次数
          fcharge             decimal(32,6),         --总台费
          fplay_time          decimal(32),         --用户玩牌时长
          fparty_time         decimal(32),         --牌局时长
          fwin_amt            decimal(32),         --用户赢得的游戏币
          flose_amt           decimal(32),          --用户输掉的游戏币
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
            insert overwrite table dim.gameparty_stream
            partition( dt="%(statdate)s" )
            select  fbpid,
                    fgame_id,
                    max(fchannel_code) fchannel_code,
                    ftbl_id,
                    finning_id,
                    fpname,
                    fsubname,
                    fparty_type,
                    max(fpalyer_cnt) fpalyer_cnt,
                    fblind_1,
                    count(1) fparty_num,
                    sum(fcharge) fcharge,
                    sum(case when ug.fs_timer = '1970-01-01 00:00:00' then 0
                        when ug.fe_timer = '1970-01-01 00:00:00' then 0
                        when unix_timestamp(fe_timer)-unix_timestamp(fs_timer) >=86400 then 0
                        else unix_timestamp(ug.fe_timer)-unix_timestamp(ug.fs_timer)
                        end) fplay_time,
                    max( case when fs_timer = '1970-01-01 00:00:00' then 0
                            when fe_timer = '1970-01-01 00:00:00' then 0
                            when unix_timestamp(fe_timer)-unix_timestamp(fs_timer) >=86400 then 0
                        else unix_timestamp(fe_timer)-unix_timestamp(fs_timer)
                        end ) fparty_time,
                    sum( case when ug.fgamecoins > 0 then ug.fgamecoins else 0 end ) fwin_amt,
                    sum( case when ug.fgamecoins < 0 then abs(ug.fgamecoins) else 0 end ) flose_amt,
                    fgamefsk,
                    fplatformfsk ,
                    fhallfsk ,
                    fterminaltypefsk ,
                    fversionfsk ,
                    hallmode
                from dim.user_gameparty_stream ug
                where dt = '%(statdate)s'
                group by fbpid,
                         ftbl_id,
                         finning_id,
                         fgame_id,
                         fpname,
                         fsubname,
                         fparty_type,
                         fblind_1,
                         fgamefsk  ,
                        fplatformfsk ,
                        fhallfsk ,
                        fterminaltypefsk ,
                        fversionfsk ,
                        hallmode
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res


#生成统计实例
a = load_gameparty_stream(sys.argv[1:])
a()
