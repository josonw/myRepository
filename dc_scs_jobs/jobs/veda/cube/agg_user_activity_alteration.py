#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserActivityAlteration(BaseStatModel):
    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS veda.user_activity_alteration
            (
              fgamefsk BIGINT COMMENT '游戏ID',
              fgamename STRING COMMENT '游戏名称',
              fplatformfsk BIGINT COMMENT '平台ID',
              fplatformname STRING COMMENT '平台名称',
              fuid BIGINT COMMENT '用户ID',
              game_duration_avg30day INT COMMENT '近30日平均游戏时长（分钟）',
              game_duration_avg15day INT COMMENT '近15日平均游戏时长（分钟）',
              game_duration_avg7day INT COMMENT '近7日平均游戏时长（分钟）',
              game_duration_avg3day INT COMMENT '近3日平均游戏时长（分钟）',
              trend STRING COMMENT '活跃趋势',
              alteration DECIMAL(3,2) COMMENT '活跃度变化率'
            )
            COMMENT '用户活跃度变化情况'
            STORED AS PARQUET
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        
        hql = """
            CREATE TABLE IF NOT EXISTS veda.dfqp_subgame_user_activity_alteration
            (
              fplatformfsk BIGINT COMMENT '平台ID',
              fplatformname STRING COMMENT '平台名称',
              fgame_id INT COMMENT '子游戏ID',
              fpname STRING COMMENT '子游戏名称',
              fuid bigint comment '用户ID',
              game_duration_avg30day INT COMMENT '近30日平均游戏时长（分钟）',
              game_duration_avg15day INT COMMENT '近15日平均游戏时长（分钟）',
              game_duration_avg7day INT COMMENT '近7日平均游戏时长（分钟）',
              game_duration_avg3day INT COMMENT '近3日平均游戏时长（分钟）',
              trend STRING COMMENT '活跃趋势',
              alteration DECIMAL(3,2) COMMENT '活跃度变化率'
            )
            COMMENT '地方棋牌子游戏用户活跃度变化情况'
            STORED AS PARQUET
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        hql = """
            with v as
             (select fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fuid,
                     ceil(sum(case
                                when dt >= date_sub('%(statdate)s', 29) and dt <= '%(statdate)s' then
                                 play_duration / 60
                                else
                                 0
                              end) / 30) as duration_avg30day,
                     ceil(sum(case
                                when dt >= date_sub('%(statdate)s', 14) and dt <= '%(statdate)s' then
                                 play_duration / 60
                                else
                                 0
                              end) / 15) as duration_avg15day,
                     ceil(sum(case
                                when dt >= date_sub('%(statdate)s', 6) and dt <= '%(statdate)s' then
                                 play_duration / 60
                                else
                                 0
                              end) / 7) as duration_avg7day,
                     ceil(sum(case
                                when dt >= date_sub('%(statdate)s', 2) and dt <= '%(statdate)s' then
                                 play_duration / 60
                                else
                                 0
                              end) / 3) as duration_avg3day
                from veda.user_label_day
               where dt >= date_sub('%(statdate)s', 29)
                 and dt <= '%(statdate)s'
               group by fgamefsk, fgamename, fplatformfsk, fplatformname, fuid)
            insert overwrite table veda.user_activity_alteration
            select fgamefsk,
                   fgamename,
                   fplatformfsk,
                   fplatformname,
                   fuid,
                   duration_avg30day,
                   duration_avg15day,
                   duration_avg7day,
                   duration_avg3day,
                   concat(case
                            when duration_avg30day < duration_avg15day then
                             '↗'
                            when duration_avg30day > duration_avg15day then
                             '↘'
                            when duration_avg30day = duration_avg15day and
                                 duration_avg30day != 0 then
                             '→'
                            else
                             ''
                          end,
                          case
                            when duration_avg15day < duration_avg7day then
                             '↗'
                            when duration_avg15day > duration_avg7day then
                             '↘'
                            when duration_avg15day = duration_avg7day and
                                 duration_avg15day != 0 then
                             '→'
                            else
                             ''
                          end,
                          case
                            when duration_avg7day < duration_avg3day then
                             '↗'
                            when duration_avg7day > duration_avg3day then
                             '↘'
                            when duration_avg7day = duration_avg3day and duration_avg7day != 0 then
                             '→'
                            else
                             ''
                          end) as trend,
                   round((duration_avg15day - duration_avg30day) / duration_avg30day * 0.4 +
                         nvl((duration_avg7day - duration_avg15day) / duration_avg15day,
                             -1) * 0.4 +
                         nvl((duration_avg3day - duration_avg7day) / duration_avg7day,
                             -1) * 0.2,
                         2) as alteration
              from v
             where duration_avg30day > 0
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        
        hql = """
            with v as
             (select fplatformfsk,
                     fplatformname,
                     fgame_id,
                     fpname,
                     fuid,
                     ceil(sum(case
                                when dt >= date_sub('%(statdate)s', 29) and dt <= '%(statdate)s' then
                                 play_duration / 60
                                else
                                 0
                              end) / 30) as duration_avg30day,
                     ceil(sum(case
                                when dt >= date_sub('%(statdate)s', 14) and dt <= '%(statdate)s' then
                                 play_duration / 60
                                else
                                 0
                              end) / 15) as duration_avg15day,
                     ceil(sum(case
                                when dt >= date_sub('%(statdate)s', 6) and dt <= '%(statdate)s' then
                                 play_duration / 60
                                else
                                 0
                              end) / 7) as duration_avg7day,
                     ceil(sum(case
                                when dt >= date_sub('%(statdate)s', 2) and dt <= '%(statdate)s' then
                                 play_duration / 60
                                else
                                 0
                              end) / 3) as duration_avg3day
                from veda.dfqp_subgame_user_label_day
               where dt >= date_sub('%(statdate)s', 29)
                 and dt <= '%(statdate)s'
               group by fplatformfsk, fplatformname, fgame_id, fpname, fuid)
            insert overwrite table veda.dfqp_subgame_user_activity_alteration
            select fplatformfsk,
                   fplatformname,
                   fgame_id,
                   fpname,
                   fuid,
                   duration_avg30day,
                   duration_avg15day,
                   duration_avg7day,
                   duration_avg3day,
                   concat(case
                            when duration_avg30day < duration_avg15day then
                             '↗'
                            when duration_avg30day > duration_avg15day then
                             '↘'
                            when duration_avg30day = duration_avg15day and
                                 duration_avg30day != 0 then
                             '→'
                            else
                             ''
                          end,
                          case
                            when duration_avg15day < duration_avg7day then
                             '↗'
                            when duration_avg15day > duration_avg7day then
                             '↘'
                            when duration_avg15day = duration_avg7day and
                                 duration_avg15day != 0 then
                             '→'
                            else
                             ''
                          end,
                          case
                            when duration_avg7day < duration_avg3day then
                             '↗'
                            when duration_avg7day > duration_avg3day then
                             '↘'
                            when duration_avg7day = duration_avg3day and duration_avg7day != 0 then
                             '→'
                            else
                             ''
                          end) as trend,
                   round((duration_avg15day - duration_avg30day) / duration_avg30day * 0.4 +
                         nvl((duration_avg7day - duration_avg15day) / duration_avg15day,
                             -1) * 0.4 +
                         nvl((duration_avg3day - duration_avg7day) / duration_avg7day,
                             -1) * 0.2,
                         2) as alteration
              from v
             where duration_avg30day > 0
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
a = UserActivityAlteration(sys.argv[1:])
a()