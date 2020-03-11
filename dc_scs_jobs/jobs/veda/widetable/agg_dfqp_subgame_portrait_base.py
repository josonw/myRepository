# -*- coding: utf-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_dfqp_subgame_portrait_base(BaseStatModel):
    """地方棋牌基础牌局(不含比赛)画像数据"""
    def create_tab(self):
        hql = """
            create table if not exists veda.dfqp_game_portrait_base
            (
             game_id int comment '子游戏ID', 
             game_name string comment '子游戏名称', 
             play_players bigint comment '玩牌人数(去重)', 
             play_new_enter_players bigint comment '玩牌新进入人数(去重)', 
             play_new_enter_players_newregistration bigint comment '玩牌新进入数(去重)-新注册用户', 
             play_new_enter_players_registered bigint comment '玩牌新进入数(去重)-老用户', 
             play_new_players bigint comment '玩牌新增人数(去重)', 
             play_new_players_newregistration bigint comment '玩牌新增人数(去重)-新注册用户', 
             play_new_players_registered bigint comment '玩牌新增人数(去重)-老用户', 
             play_inning_num bigint comment '玩牌局数', 
             play_num bigint comment '玩牌人次', 
             play_time bigint comment '玩牌时长', 
             play_time_inning_avg bigint comment '平均每局玩牌时长', 
             play_num_avg bigint comment '平均每人玩牌局数', 
             play_time_player_avg bigint comment '平均每人玩牌时长', 
             rupt_num bigint comment '破产次数', 
             play_players_gamecoins bigint comment '玩牌人数(去重)-银币', 
             play_new_players_gamecoins bigint comment '玩牌新增人数(去重)-银币', 
             play_new_players_newregistration_gamecoins bigint comment '玩牌新增人数(去重)-新注册用户-银币', 
             play_new_players_registered_gamecoins bigint comment '玩牌新增人数(去重)-老用户-银币', 
             play_inning_num_gamecoins bigint comment '玩牌局数-银币', 
             play_num_gamecoins bigint comment '玩牌人次-银币', 
             play_time_gamecoins bigint comment '玩牌时长-银币', 
             play_charge_gamecoins decimal(32,6) comment '玩牌台费-银币', 
             play_amt_gamecoins decimal(32,6) comment '玩牌净输赢-银币', 
             play_amt_total_gamecoins decimal(32,6) comment '玩牌总输赢(输赢绝对值相加)-银币', 
             play_win_amt_gamecoins decimal(32,6) comment '玩牌赢钱-银币', 
             play_lose_amt_gamecoins decimal(32,6) comment '玩牌输钱-银币', 
             play_win_amt_max_gamecoins decimal(32,6) comment '玩牌最大赢钱-银币', 
             play_lose_amt_max_gamecoins decimal(32,6) comment '玩牌最大输钱-银币', 
             rupt_num_gamecoins bigint comment '破产次数-银币', 
             play_time_inning_avg_gamecoins bigint comment '平均每局玩牌时长-银币', 
             play_num_avg_gamecoins bigint comment '平均每人玩牌局数-银币', 
             play_time_player_avg_gamecoins bigint comment '平均每人玩牌时长-银币',
             play_charge_avg_gamecoins decimal(32,6) comment '平均每人台费-银币', 
             play_amt_avg_gamecoins decimal(32,6) comment '平均每人净输赢-银币', 
             play_amt_total_avg_gamecoins decimal(32,6) comment '平均每人总输赢(输赢绝对值相加)-银币', 
             play_win_amt_avg_gamecoins decimal(32,6) comment '平均每人赢钱-银币', 
             play_lose_amt_avg_gamecoins decimal(32,6) comment '平均每人输钱-银币', 
             rupt_num_avg_gamecoins bigint comment '平均每人破产次数-银币', 
             play_players_gold bigint comment '玩牌人数(去重)-金条', 
             play_new_players_gold bigint comment '玩牌新增人数(去重)-金条', 
             play_new_players_newregistration_gold bigint comment '玩牌新增人数(去重)-新注册用户-金条', 
             play_new_players_registered_gold bigint comment '玩牌新增人数(去重)-老用户-金条', 
             play_inning_num_gold bigint comment '玩牌局数-金条', 
             play_num_gold bigint comment '玩牌人次-金条', 
             play_time_gold bigint comment '玩牌时长-金条', 
             play_charge_gold decimal(32,6) comment '玩牌台费-金条', 
             play_amt_gold decimal(32,6) comment '玩牌净输赢-金条', 
             play_amt_total_gold decimal(32,6) comment '玩牌总输赢(输赢绝对值相加)-金条', 
             play_win_amt_gold decimal(32,6) comment '玩牌赢钱-金条', 
             play_lose_amt_gold decimal(32,6) comment '玩牌输钱-金条', 
             play_win_amt_max_gold decimal(32,6) comment '玩牌最大赢钱-金条', 
             play_lose_amt_max_gold decimal(32,6) comment '玩牌最大输钱-金条', 
             rupt_num_gold bigint comment '破产次数-金条', 
             play_time_inning_avg_gold bigint comment '平均每局玩牌时长-金条', 
             play_num_avg_gold bigint comment '平均每人玩牌局数-金条', 
             play_time_player_avg_gold bigint comment '平均每人玩牌时长-金条',
             play_charge_avg_gold decimal(32,6) comment '平均每人台费-金条', 
             play_amt_avg_gold decimal(32,6) comment '平均每人净输赢-金条', 
             play_amt_total_avg_gold decimal(32,6) comment '平均每人总输赢(输赢绝对值相加)-金条', 
             play_win_amt_avg_gold decimal(32,6) comment '平均每人赢钱-金条', 
             play_lose_amt_avg_gold decimal(32,6) comment '平均每人输钱-金条', 
             rupt_num_avg_gold bigint comment '平均每人破产次数-金条', 
             play_players_integral bigint comment '玩牌人数(去重)-积分', 
             play_new_players_integral bigint comment '玩牌新增人数(去重)-积分', 
             play_new_players_newregistration_integral bigint comment '玩牌新增人数(去重)-新注册用户-积分', 
             play_new_players_registered_integral bigint comment '玩牌新增人数(去重)-老用户-积分', 
             play_inning_num_integral bigint comment '玩牌局数-积分', 
             play_num_integral bigint comment '玩牌人次-积分', 
             play_time_integral bigint comment '玩牌时长-积分', 
             play_charge_integral decimal(32,6) comment '玩牌台费-积分', 
             play_amt_integral decimal(32,6) comment '玩牌净输赢-积分', 
             play_amt_total_integral decimal(32,6) comment '玩牌总输赢(输赢绝对值相加)-积分', 
             play_win_amt_integral decimal(32,6) comment '玩牌赢钱-积分', 
             play_lose_amt_integral decimal(32,6) comment '玩牌输钱-积分', 
             play_win_amt_max_integral decimal(32,6) comment '玩牌最大赢钱-积分', 
             play_lose_amt_max_integral decimal(32,6) comment '玩牌最大输钱-积分', 
             rupt_num_integral bigint comment '破产次数-积分', 
             play_time_inning_avg_integral bigint comment '平均每局玩牌时长-积分', 
             play_num_avg_integral bigint comment '平均每人玩牌局数-积分', 
             play_time_player_avg_integral bigint comment '平均每人玩牌时长-积分',
             play_charge_avg_integral decimal(32,6) comment '平均每人台费-积分', 
             play_amt_avg_integral decimal(32,6) comment '平均每人净输赢-积分', 
             play_amt_total_avg_integral decimal(32,6) comment '平均每人总输赢(输赢绝对值相加)-积分', 
             play_win_amt_avg_integral decimal(32,6) comment '平均每人赢钱-积分', 
             play_lose_amt_avg_integral decimal(32,6) comment '平均每人输钱-积分', 
             rupt_num_avg_integral bigint comment '平均每人破产次数-积分' 
            ) comment '地方棋牌基础牌局(到子游戏，不含比赛)画像数据'
            partitioned by (dt string);


            create table if not exists veda.dfqp_subgame_portrait_base
            (
             game_id int comment '子游戏ID', 
             game_name string comment '子游戏名称', 
             subname string comment '子游戏二级场次（比赛取三级场次）', 
             hallname string comment '大厅名称',
             play_players bigint comment '玩牌人数(去重)', 
             play_new_enter_players bigint comment '玩牌新进入人数(去重)', 
             play_new_enter_players_newregistration bigint comment '玩牌新进入数(去重)-新注册用户', 
             play_new_enter_players_registered bigint comment '玩牌新进入数(去重)-老用户', 
             play_new_players bigint comment '玩牌新增人数(去重)', 
             play_new_players_newregistration bigint comment '玩牌新增人数(去重)-新注册用户', 
             play_new_players_registered bigint comment '玩牌新增人数(去重)-老用户', 
             play_inning_num bigint comment '玩牌局数', 
             play_num bigint comment '玩牌人次', 
             play_time bigint comment '玩牌时长', 
             play_time_inning_avg bigint comment '平均每局玩牌时长', 
             play_num_avg bigint comment '平均每人玩牌局数', 
             play_time_player_avg bigint comment '平均每人玩牌时长', 
             rupt_num bigint comment '破产次数', 
             play_players_gamecoins bigint comment '玩牌人数(去重)-银币', 
             play_new_players_gamecoins bigint comment '玩牌新增人数(去重)-银币', 
             play_new_players_newregistration_gamecoins bigint comment '玩牌新增人数(去重)-新注册用户-银币', 
             play_new_players_registered_gamecoins bigint comment '玩牌新增人数(去重)-老用户-银币', 
             play_inning_num_gamecoins bigint comment '玩牌局数-银币', 
             play_num_gamecoins bigint comment '玩牌人次-银币', 
             play_time_gamecoins bigint comment '玩牌时长-银币', 
             play_charge_gamecoins decimal(32,6) comment '玩牌台费-银币', 
             play_amt_gamecoins decimal(32,6) comment '玩牌净输赢-银币', 
             play_amt_total_gamecoins decimal(32,6) comment '玩牌总输赢(输赢绝对值相加)-银币', 
             play_win_amt_gamecoins decimal(32,6) comment '玩牌赢钱-银币', 
             play_lose_amt_gamecoins decimal(32,6) comment '玩牌输钱-银币', 
             play_win_amt_max_gamecoins decimal(32,6) comment '玩牌最大赢钱-银币', 
             play_lose_amt_max_gamecoins decimal(32,6) comment '玩牌最大输钱-银币', 
             rupt_num_gamecoins bigint comment '破产次数-银币', 
             play_time_inning_avg_gamecoins bigint comment '平均每局玩牌时长-银币', 
             play_num_avg_gamecoins bigint comment '平均每人玩牌局数-银币', 
             play_time_player_avg_gamecoins bigint comment '平均每人玩牌时长-银币',
             play_charge_avg_gamecoins decimal(32,6) comment '平均每人台费-银币', 
             play_amt_avg_gamecoins decimal(32,6) comment '平均每人净输赢-银币', 
             play_amt_total_avg_gamecoins decimal(32,6) comment '平均每人总输赢(输赢绝对值相加)-银币', 
             play_win_amt_avg_gamecoins decimal(32,6) comment '平均每人赢钱-银币', 
             play_lose_amt_avg_gamecoins decimal(32,6) comment '平均每人输钱-银币', 
             rupt_num_avg_gamecoins bigint comment '平均每人破产次数-银币', 
             play_players_gold bigint comment '玩牌人数(去重)-金条', 
             play_new_players_gold bigint comment '玩牌新增人数(去重)-金条', 
             play_new_players_newregistration_gold bigint comment '玩牌新增人数(去重)-新注册用户-金条', 
             play_new_players_registered_gold bigint comment '玩牌新增人数(去重)-老用户-金条', 
             play_inning_num_gold bigint comment '玩牌局数-金条', 
             play_num_gold bigint comment '玩牌人次-金条', 
             play_time_gold bigint comment '玩牌时长-金条', 
             play_charge_gold decimal(32,6) comment '玩牌台费-金条', 
             play_amt_gold decimal(32,6) comment '玩牌净输赢-金条', 
             play_amt_total_gold decimal(32,6) comment '玩牌总输赢(输赢绝对值相加)-金条', 
             play_win_amt_gold decimal(32,6) comment '玩牌赢钱-金条', 
             play_lose_amt_gold decimal(32,6) comment '玩牌输钱-金条', 
             play_win_amt_max_gold decimal(32,6) comment '玩牌最大赢钱-金条', 
             play_lose_amt_max_gold decimal(32,6) comment '玩牌最大输钱-金条', 
             rupt_num_gold bigint comment '破产次数-金条', 
             play_time_inning_avg_gold bigint comment '平均每局玩牌时长-金条', 
             play_num_avg_gold bigint comment '平均每人玩牌局数-金条', 
             play_time_player_avg_gold bigint comment '平均每人玩牌时长-金条',
             play_charge_avg_gold decimal(32,6) comment '平均每人台费-金条', 
             play_amt_avg_gold decimal(32,6) comment '平均每人净输赢-金条', 
             play_amt_total_avg_gold decimal(32,6) comment '平均每人总输赢(输赢绝对值相加)-金条', 
             play_win_amt_avg_gold decimal(32,6) comment '平均每人赢钱-金条', 
             play_lose_amt_avg_gold decimal(32,6) comment '平均每人输钱-金条', 
             rupt_num_avg_gold bigint comment '平均每人破产次数-金条', 
             play_players_integral bigint comment '玩牌人数(去重)-积分', 
             play_new_players_integral bigint comment '玩牌新增人数(去重)-积分', 
             play_new_players_newregistration_integral bigint comment '玩牌新增人数(去重)-新注册用户-积分', 
             play_new_players_registered_integral bigint comment '玩牌新增人数(去重)-老用户-积分', 
             play_inning_num_integral bigint comment '玩牌局数-积分', 
             play_num_integral bigint comment '玩牌人次-积分', 
             play_time_integral bigint comment '玩牌时长-积分', 
             play_charge_integral decimal(32,6) comment '玩牌台费-积分', 
             play_amt_integral decimal(32,6) comment '玩牌净输赢-积分', 
             play_amt_total_integral decimal(32,6) comment '玩牌总输赢(输赢绝对值相加)-积分', 
             play_win_amt_integral decimal(32,6) comment '玩牌赢钱-积分', 
             play_lose_amt_integral decimal(32,6) comment '玩牌输钱-积分', 
             play_win_amt_max_integral decimal(32,6) comment '玩牌最大赢钱-积分', 
             play_lose_amt_max_integral decimal(32,6) comment '玩牌最大输钱-积分', 
             rupt_num_integral bigint comment '破产次数-积分', 
             play_time_inning_avg_integral bigint comment '平均每局玩牌时长-积分', 
             play_num_avg_integral bigint comment '平均每人玩牌局数-积分', 
             play_time_player_avg_integral bigint comment '平均每人玩牌时长-积分',
             play_charge_avg_integral decimal(32,6) comment '平均每人台费-积分', 
             play_amt_avg_integral decimal(32,6) comment '平均每人净输赢-积分', 
             play_amt_total_avg_integral decimal(32,6) comment '平均每人总输赢(输赢绝对值相加)-积分', 
             play_win_amt_avg_integral decimal(32,6) comment '平均每人赢钱-积分', 
             play_lose_amt_avg_integral decimal(32,6) comment '平均每人输钱-积分', 
             rupt_num_avg_integral bigint comment '平均每人破产次数-积分' 
            ) comment '地方棋牌基础牌局(到场次，不含比赛)画像数据'
            partitioned by (dt string)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            insert overwrite table veda.dfqp_game_portrait_base
            partition (dt='%(statdate)s')
            select /*+ STREAMTABLE(T1) */
            t1.fgame_id as game_id,
            t1.fpname as game_name,
            count(distinct t1.fuid) as play_players,
            count(distinct(case
                              when t2.new_enter = 'Y' and nvl(t1.ffirst_play, 0) = 0 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_enter_players,
            count(distinct(case
                              when t2.new_regedit = 'Y' and t2.new_enter = 'Y' and
                                   nvl(t1.ffirst_play, 0) = 0 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_enter_players_newregistration,
            count(distinct(case
                              when t2.new_regedit = 'N' and t2.new_enter = 'Y' and
                                   nvl(t1.ffirst_play, 0) = 0 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_enter_players_registered,
            count(distinct(case
                              when t1.ffirst_play = 1 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_players,
            count(distinct(case
                              when t2.new_regedit = 'Y' and t1.ffirst_play = 1 then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_newregistration,
            count(distinct(case
                              when t2.new_regedit = 'N' and t1.ffirst_play = 1 then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_registered,
            count(distinct t1.finning_id) as play_inning_num,
            count(1) as play_num,
            sum(unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)) as play_time,
            sum(unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)) /
            count(distinct t1.finning_id) as play_time_inning_avg,
            count(distinct t1.finning_id) / count(distinct t1.fuid) as play_num_avg,
            sum(unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)) /
            count(distinct t1.fuid) as play_time_player_avg,
            sum(case
                   when fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) as rupt_num,
            count(distinct(case
                              when t1.fcoins_type = 1 then
                               t1.fuid
                              else
                               null
                            end)) as play_players_gamecoins,
            count(distinct(case
                              when t1.fcoins_type = 1 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_players_gamecoins,
            count(distinct(case
                              when t1.fcoins_type = 1 and t2.new_regedit = 'Y' then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_newregistration_gamecoins,
            count(distinct(case
                              when t1.fcoins_type = 1 and t2.new_regedit = 'N' and t2.new_enter = 'Y' then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_registered_gamecoins,
            count(distinct(case
                              when t1.fcoins_type = 1 then
                               t1.finning_id
                              else
                               null
                            end)) as play_inning_num_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    1
                   else
                    0
                 end) as play_num_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) as play_time_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    t1.fcharge
                   else
                    0
                 end) as play_charge_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_amt_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    abs(t1.fgamecoins)
                   else
                    0
                 end) as play_amt_total_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 and t1.fgamecoins > 0 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_win_amt_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 and t1.fgamecoins < 0 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_lose_amt_gamecoins,
            max(case
                   when t1.fcoins_type = 1 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_win_amt_max_gamecoins,
            min(case
                   when t1.fcoins_type = 1 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_lose_amt_max_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 and fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) as rupt_num_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.finning_id
                                         else
                                          null
                                       end)) as play_time_inning_avg_gamecoins,
            count(distinct(case
                              when t1.fcoins_type = 1 then
                               t1.finning_id
                              else
                               null
                            end)) / count(distinct(case
                                                     when t1.fcoins_type = 1 then
                                                      t1.fuid
                                                     else
                                                      null
                                                   end)) as play_num_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_time_player_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    t1.fcharge
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_charge_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_amt_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    abs(t1.fgamecoins)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_amt_total_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 and t1.fgamecoins > 0 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_win_amt_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 and t1.fgamecoins < 0 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_lose_amt_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 and fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as rupt_num_avg_gamecoins,
            count(distinct(case
                              when t1.fcoins_type = 11 then
                               t1.fuid
                              else
                               null
                            end)) as play_players_gold,
            count(distinct(case
                              when t1.fcoins_type = 11 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_players_gold,
            count(distinct(case
                              when t1.fcoins_type = 11 and t2.new_regedit = 'Y' then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_newregistration_gold,
            count(distinct(case
                              when t1.fcoins_type = 11 and t2.new_regedit = 'N' and t2.new_enter = 'Y' then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_registered_gold,
            count(distinct(case
                              when t1.fcoins_type = 11 then
                               t1.finning_id
                              else
                               null
                            end)) as play_inning_num_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    1
                   else
                    0
                 end) as play_num_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) as play_time_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    t1.fcharge
                   else
                    0
                 end) as play_charge_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_amt_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    abs(t1.fgamecoins)
                   else
                    0
                 end) as play_amt_total_gold,
            sum(case
                   when t1.fcoins_type = 11 and t1.fgamecoins > 0 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_win_amt_gold,
            sum(case
                   when t1.fcoins_type = 11 and t1.fgamecoins < 0 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_lose_amt_gold,
            max(case
                   when t1.fcoins_type = 11 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_win_amt_max_gold,
            min(case
                   when t1.fcoins_type = 11 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_lose_amt_max_gold,
            sum(case
                   when t1.fcoins_type = 11 and fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) as rupt_num_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.finning_id
                                         else
                                          null
                                       end)) as play_time_inning_avg_gold,
            count(distinct(case
                              when t1.fcoins_type = 11 then
                               t1.finning_id
                              else
                               null
                            end)) / count(distinct(case
                                                     when t1.fcoins_type = 11 then
                                                      t1.fuid
                                                     else
                                                      null
                                                   end)) as play_num_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_time_player_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    t1.fcharge
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_charge_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_amt_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    abs(t1.fgamecoins)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_amt_total_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 and t1.fgamecoins > 0 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_win_amt_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 and t1.fgamecoins < 0 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_lose_amt_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 and fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as rupt_num_avg_gold,
            count(distinct(case
                              when t1.fcoins_type = 3 then
                               t1.fuid
                              else
                               null
                            end)) as play_players_integral,
            count(distinct(case
                              when t1.fcoins_type = 3 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_players_integral,
            count(distinct(case
                              when t1.fcoins_type = 3 and t2.new_regedit = 'Y' then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_newregistration_integral,
            count(distinct(case
                              when t1.fcoins_type = 3 and t2.new_regedit = 'N' and t2.new_enter = 'Y' then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_registered_integral,
            count(distinct(case
                              when t1.fcoins_type = 3 then
                               t1.finning_id
                              else
                               null
                            end)) as play_inning_num_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    1
                   else
                    0
                 end) as play_num_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) as play_time_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    t1.fcharge
                   else
                    0
                 end) as play_charge_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_amt_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    abs(t1.fgamecoins)
                   else
                    0
                 end) as play_amt_total_integral,
            sum(case
                   when t1.fcoins_type = 3 and t1.fgamecoins > 0 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_win_amt_integral,
            sum(case
                   when t1.fcoins_type = 3 and t1.fgamecoins < 0 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_lose_amt_integral,
            max(case
                   when t1.fcoins_type = 3 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_win_amt_max_integral,
            min(case
                   when t1.fcoins_type = 3 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_lose_amt_max_integral,
            sum(case
                   when t1.fcoins_type = 3 and fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) as rupt_num_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.finning_id
                                         else
                                          null
                                       end)) as play_time_inning_avg_integral,
            count(distinct(case
                              when t1.fcoins_type = 3 then
                               t1.finning_id
                              else
                               null
                            end)) / count(distinct(case
                                                     when t1.fcoins_type = 3 then
                                                      t1.fuid
                                                     else
                                                      null
                                                   end)) as play_num_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_time_player_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    t1.fcharge
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_charge_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_amt_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    abs(t1.fgamecoins)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_amt_total_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 and t1.fgamecoins > 0 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_win_amt_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 and t1.fgamecoins < 0 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_lose_amt_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 and fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as rupt_num_avg_integral
              from stage_dfqp.user_gameparty_stream t1
              left join stage_dfqp.dfqp_user_register_enter t2
                on (t1.fuid = t2.fuid and t1.dt = t2.dt)
            where t1.dt = '%(statdate)s'
            group by t1.fgame_id, t1.fpname;


            insert overwrite table veda.dfqp_subgame_portrait_base
            partition (dt='%(statdate)s')
            select /*+ STREAMTABLE(T1) */
            t1.fgame_id as game_id,
            t1.fpname as game_name,
            t1.fsubname as subname,
            t1.fhallname as hallname,
            count(distinct t1.fuid) as play_players,
            count(distinct(case
                              when t2.new_enter = 'Y' and nvl(t1.ffirst_play, 0) = 0 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_enter_players,
            count(distinct(case
                              when t2.new_regedit = 'Y' and t2.new_enter = 'Y' and
                                   nvl(t1.ffirst_play, 0) = 0 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_enter_players_newregistration,
            count(distinct(case
                              when t2.new_regedit = 'N' and t2.new_enter = 'Y' and
                                   nvl(t1.ffirst_play, 0) = 0 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_enter_players_registered,
            count(distinct(case
                              when t1.ffirst_play = 1 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_players,
            count(distinct(case
                              when t2.new_regedit = 'Y' and t1.ffirst_play = 1 then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_newregistration,
            count(distinct(case
                              when t2.new_regedit = 'N' and t1.ffirst_play = 1 then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_registered,
            count(distinct t1.finning_id) as play_inning_num,
            count(1) as play_num,
            sum(unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)) as play_time,
            sum(unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)) /
            count(distinct t1.finning_id) as play_time_inning_avg,
            count(distinct t1.finning_id) / count(distinct t1.fuid) as play_num_avg,
            sum(unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)) /
            count(distinct t1.fuid) as play_time_player_avg,
            sum(case
                   when fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) as rupt_num,
            count(distinct(case
                              when t1.fcoins_type = 1 then
                               t1.fuid
                              else
                               null
                            end)) as play_players_gamecoins,
            count(distinct(case
                              when t1.fcoins_type = 1 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_players_gamecoins,
            count(distinct(case
                              when t1.fcoins_type = 1 and t2.new_regedit = 'Y' then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_newregistration_gamecoins,
            count(distinct(case
                              when t1.fcoins_type = 1 and t2.new_regedit = 'N' and t2.new_enter = 'Y' then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_registered_gamecoins,
            count(distinct(case
                              when t1.fcoins_type = 1 then
                               t1.finning_id
                              else
                               null
                            end)) as play_inning_num_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    1
                   else
                    0
                 end) as play_num_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) as play_time_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    t1.fcharge
                   else
                    0
                 end) as play_charge_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_amt_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    abs(t1.fgamecoins)
                   else
                    0
                 end) as play_amt_total_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 and t1.fgamecoins > 0 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_win_amt_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 and t1.fgamecoins < 0 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_lose_amt_gamecoins,
            max(case
                   when t1.fcoins_type = 1 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_win_amt_max_gamecoins,
            min(case
                   when t1.fcoins_type = 1 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_lose_amt_max_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 and fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) as rupt_num_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.finning_id
                                         else
                                          null
                                       end)) as play_time_inning_avg_gamecoins,
            count(distinct(case
                              when t1.fcoins_type = 1 then
                               t1.finning_id
                              else
                               null
                            end)) / count(distinct(case
                                                     when t1.fcoins_type = 1 then
                                                      t1.fuid
                                                     else
                                                      null
                                                   end)) as play_num_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_time_player_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    t1.fcharge
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_charge_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_amt_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 then
                    abs(t1.fgamecoins)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_amt_total_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 and t1.fgamecoins > 0 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_win_amt_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 and t1.fgamecoins < 0 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_lose_amt_avg_gamecoins,
            sum(case
                   when t1.fcoins_type = 1 and fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) / count(distinct(case
                                         when t1.fcoins_type = 1 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as rupt_num_avg_gamecoins,
            count(distinct(case
                              when t1.fcoins_type = 11 then
                               t1.fuid
                              else
                               null
                            end)) as play_players_gold,
            count(distinct(case
                              when t1.fcoins_type = 11 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_players_gold,
            count(distinct(case
                              when t1.fcoins_type = 11 and t2.new_regedit = 'Y' then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_newregistration_gold,
            count(distinct(case
                              when t1.fcoins_type = 11 and t2.new_regedit = 'N' and t2.new_enter = 'Y' then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_registered_gold,
            count(distinct(case
                              when t1.fcoins_type = 11 then
                               t1.finning_id
                              else
                               null
                            end)) as play_inning_num_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    1
                   else
                    0
                 end) as play_num_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) as play_time_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    t1.fcharge
                   else
                    0
                 end) as play_charge_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_amt_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    abs(t1.fgamecoins)
                   else
                    0
                 end) as play_amt_total_gold,
            sum(case
                   when t1.fcoins_type = 11 and t1.fgamecoins > 0 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_win_amt_gold,
            sum(case
                   when t1.fcoins_type = 11 and t1.fgamecoins < 0 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_lose_amt_gold,
            max(case
                   when t1.fcoins_type = 11 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_win_amt_max_gold,
            min(case
                   when t1.fcoins_type = 11 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_lose_amt_max_gold,
            sum(case
                   when t1.fcoins_type = 11 and fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) as rupt_num_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.finning_id
                                         else
                                          null
                                       end)) as play_time_inning_avg_gold,
            count(distinct(case
                              when t1.fcoins_type = 11 then
                               t1.finning_id
                              else
                               null
                            end)) / count(distinct(case
                                                     when t1.fcoins_type = 11 then
                                                      t1.fuid
                                                     else
                                                      null
                                                   end)) as play_num_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_time_player_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    t1.fcharge
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_charge_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_amt_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 then
                    abs(t1.fgamecoins)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_amt_total_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 and t1.fgamecoins > 0 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_win_amt_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 and t1.fgamecoins < 0 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_lose_amt_avg_gold,
            sum(case
                   when t1.fcoins_type = 11 and fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) / count(distinct(case
                                         when t1.fcoins_type = 11 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as rupt_num_avg_gold,
            count(distinct(case
                              when t1.fcoins_type = 3 then
                               t1.fuid
                              else
                               null
                            end)) as play_players_integral,
            count(distinct(case
                              when t1.fcoins_type = 3 then
                               t2.fuid
                              else
                               null
                            end)) as play_new_players_integral,
            count(distinct(case
                              when t1.fcoins_type = 3 and t2.new_regedit = 'Y' then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_newregistration_integral,
            count(distinct(case
                              when t1.fcoins_type = 3 and t2.new_regedit = 'N' and t2.new_enter = 'Y' then
                               t1.fuid
                              else
                               null
                            end)) as play_new_players_registered_integral,
            count(distinct(case
                              when t1.fcoins_type = 3 then
                               t1.finning_id
                              else
                               null
                            end)) as play_inning_num_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    1
                   else
                    0
                 end) as play_num_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) as play_time_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    t1.fcharge
                   else
                    0
                 end) as play_charge_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_amt_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    abs(t1.fgamecoins)
                   else
                    0
                 end) as play_amt_total_integral,
            sum(case
                   when t1.fcoins_type = 3 and t1.fgamecoins > 0 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_win_amt_integral,
            sum(case
                   when t1.fcoins_type = 3 and t1.fgamecoins < 0 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_lose_amt_integral,
            max(case
                   when t1.fcoins_type = 3 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_win_amt_max_integral,
            min(case
                   when t1.fcoins_type = 3 then
                    t1.fgamecoins
                   else
                    0
                 end) as play_lose_amt_max_integral,
            sum(case
                   when t1.fcoins_type = 3 and fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) as rupt_num_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.finning_id
                                         else
                                          null
                                       end)) as play_time_inning_avg_integral,
            count(distinct(case
                              when t1.fcoins_type = 3 then
                               t1.finning_id
                              else
                               null
                            end)) / count(distinct(case
                                                     when t1.fcoins_type = 3 then
                                                      t1.fuid
                                                     else
                                                      null
                                                   end)) as play_num_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    unix_timestamp(t1.fe_timer) - unix_timestamp(t1.fs_timer)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_time_player_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    t1.fcharge
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_charge_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_amt_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 then
                    abs(t1.fgamecoins)
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_amt_total_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 and t1.fgamecoins > 0 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_win_amt_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 and t1.fgamecoins < 0 then
                    t1.fgamecoins
                   else
                    0
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as play_lose_amt_avg_integral,
            sum(case
                   when t1.fcoins_type = 3 and fis_bankrupt is null or fis_bankrupt = 0 then
                    0
                   else
                    1
                 end) / count(distinct(case
                                         when t1.fcoins_type = 3 then
                                          t1.fuid
                                         else
                                          null
                                       end)) as rupt_num_avg_integral
              from stage_dfqp.user_gameparty_stream t1
              left join stage_dfqp.dfqp_user_register_enter t2
                on (t1.fuid = t2.fuid and t1.dt = t2.dt)
            where t1.dt = '%(statdate)s'
            group by t1.fgame_id, t1.fpname, t1.fsubname, t1.fhallname
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_dfqp_subgame_portrait_base(sys.argv[1:])
a()

