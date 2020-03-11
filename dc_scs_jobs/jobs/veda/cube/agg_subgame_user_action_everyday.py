#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class SubgameUserActionEveryday(BaseStatModel):

    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS veda.subgame_user_action_everyday
            (
              fgamefsk bigint comment '游戏ID',
              fgamename string comment '游戏名称',
              fplatformfsk bigint comment '平台ID',
              fplatformname string comment '平台名称',
              fgame_id bigint comment '子游戏ID',
              fpname string comment '子游戏名称',
              fuid bigint comment '用户ID',
              enter_count int comment '进入游戏次数',
              game_duration int comment '游戏时长（秒）',
              game_innings int comment '游戏局数',
              win_gamecoin bigint comment '赢取游戏币',
              lose_gamecoin bigint comment '输去游戏币',
              table_charge_gamecoin bigint comment '台费游戏币',
              net_gamecoin bigint comment '游戏币净输赢',
              win_gold bigint comment '赢取金条（地方棋牌）',
              lose_gold bigint comment '输去金条（地方棋牌）',
              table_charge_gold bigint comment '台费金条（地方棋牌）',
              net_gold bigint comment '金条净输赢（地方棋牌）',
              bankrupt_count int comment '破产次数',
              relieve_count int comment '领取救济次数',
              relieve_gamecoin bigint comment '领取救济游戏币',
              trustee_count int comment '托管使用次数',
              item_use_count int comment '道具使用次数',
              click_count int comment '点击次数',
              banker_count int comment '坐庄次数（庄家/地主/王）',
              discard_count int comment '弃牌次数',
              show_card_count int comment '亮牌次数',
              stand_count int comment '站起次数（德州扑克）',
              abnormal_end_count int comment '牌局非正常结束次数',
              basic_duration int comment '普通场玩牌时长（秒）',
              basic_win_innings int comment '普通场胜局数',
              basic_lose_innings int comment '普通场输局数',
              private_duration int comment '私人场玩牌时长（秒）',
              private_win_innings int comment '私人场胜局数',
              private_lose_innings int comment '私人场输局数',
              match_duration int comment '比赛场时长（秒）',
              match_count int comment '比赛次数',
              match_entry_fee bigint comment '比赛报名费（金条）',
              match_reward_count int comment '比赛场获奖次数',
              match_reward_gamecoin bigint comment '比赛场获奖游戏币',
              match_reward_gold bigint comment '比赛场获奖金条',
              match_reward_cash_value decimal(10,2) comment '比赛场获奖现金价值（人民币元）',
              game_halls string comment '当天进入大厅',
              major_hall string comment '主玩大厅',
              subnames string comment '当天进入二级场',
              major_subname string comment '主玩二级场',
              major_subname_proportion decimal(5,4) comment '主玩二级场时长占比',
              gsubnames string comment '当天进入三级场',
              major_gsubname string comment '主玩三级场',
              major_gsubname_proportion decimal(5,4) comment '主玩三级场时长占比',
              latest_game_version string comment '最后玩子游戏版本'
            )
            COMMENT '子游戏用户每天行为统计'
            PARTITIONED BY (dt STRING COMMENT '日期')
            STORED AS PARQUET
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        HQL_enter_info = """
            drop table if exists work.enter_info_temp_%(statdatenum)s ;
            create table if not exists work.enter_info_temp_%(statdatenum)s as
            select fgamefsk,
                   fgamename,
                   fplatformfsk,
                   fplatformname,
                   fgame_id,
                   fpname,
                   fuid,
                   enter_count,
                   latest_game_version
              from (select t1.fgamefsk,
                           t1.fgamename,
                           t1.fplatformfsk,
                           t1.fplatformname,
                           t2.fgame_id,
                           t2.fpname,
                           t2.fuid,
                           count(distinct t2.flts_at) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fgame_id, t2.fuid) as enter_count,
                           t2.fversion_info as latest_game_version,
                           row_number() over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fgame_id, t2.fuid order by t2.flts_at desc) ranking
                      from dim.bpid_map t1
                     inner join stage.user_enter_stg t2
                        on t1.fbpid = t2.fbpid
                     where t1.fgamename in ('地方棋牌', '德州扑克')
                       and dt = '%(statdate)s') m
             where ranking = 1
        """
        res = self.sql_exe(HQL_enter_info)
        if res != 0:
            return res

        HQL_gameparty_info = """
            drop table if exists work.gameparty_info_temp_%(statdatenum)s ;
            create table if not exists work.gameparty_info_temp_%(statdatenum)s as
            select t1.fgamefsk,
                   t1.fgamename,
                   t1.fplatformfsk,
                   t1.fplatformname,
                   t2.fgame_id,
                   t2.fpname,
                   t2.fuid,
                   sum(case
                         when unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                              10800 then
                          unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                         else
                          0
                       end) as game_duration,
                   count(distinct finning_id) as game_innings,
                   sum(case
                         when (fgamename = '地方棋牌' and coalesce(fcoins_type, 1) = 1 or
                              fgamename = '德州扑克' and coalesce(fcoins_type, 0) = 0) and
                              fgamecoins > 0 then
                          fgamecoins
                         else
                          0
                       end) as win_gamecoin,
                   sum(case
                         when (fgamename = '地方棋牌' and coalesce(fcoins_type, 1) = 1 or
                              fgamename = '德州扑克' and coalesce(fcoins_type, 0) = 0) and
                              fgamecoins < 0 then
                          abs(fgamecoins)
                         else
                          0
                       end) as lose_gamecoin,
                   sum(case
                         when fgamename = '地方棋牌' and coalesce(fcoins_type, 1) = 1 or
                              fgamename = '德州扑克' and coalesce(fcoins_type, 0) = 0 then
                          cast(fcharge as bigint)
                         else
                          0
                       end) as table_charge_gamecoin,
                   sum(case
                         when fgamename = '地方棋牌' and coalesce(fcoins_type, 1) = 1 or
                              fgamename = '德州扑克' and coalesce(fcoins_type, 0) = 0 then
                          fgamecoins
                         else
                          0
                       end) as net_gamecoin,
                   sum(case
                         when fgamename = '地方棋牌' and fcoins_type = 11 and fgamecoins > 0 then
                          fgamecoins
                         else
                          0
                       end) as win_gold,
                   sum(case
                         when fgamename = '地方棋牌' and fcoins_type = 11 and fgamecoins < 0 then
                          abs(fgamecoins)
                         else
                          0
                       end) as lose_gold,
                   sum(case
                         when fgamename = '地方棋牌' and fcoins_type = 11 then
                          cast(fcharge as bigint)
                         else
                          0
                       end) as table_charge_gold,
                   sum(case
                         when fgamename = '地方棋牌' and fcoins_type = 11 then
                          fgamecoins
                         else
                          0
                       end) as net_gold,
                   sum(ftrustee_num) as trustee_count,
                   sum(fis_king) as banker_count,
                   sum(fhas_fold) as discard_count,
                   sum(fhas_open) as show_card_count,
                   sum(has_stand) as stand_count,
                   sum(if(coalesce(fis_end, 0) != 0, 1, 0)) abnormal_end_count,
                   sum(case
                         when coalesce(fmatch_id, '0') = '0' and
                              coalesce(fmatch_cfg_id, fmatch_log_id, 0) = 0 and fsubname
                          rlike '1[234]$' and
                              unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                              10800 then
                          unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                         else
                          0
                       end) as basic_duration,
                   count(distinct(case
                                    when coalesce(fmatch_id, '0') = '0' and
                                         coalesce(fmatch_cfg_id, fmatch_log_id, 0) = 0 and fsubname
                                     rlike '1[234]$' and fgamecoins > 0 then
                                     finning_id
                                    else
                                     null
                                  end)) as basic_win_innings,
                   count(distinct(case
                                    when coalesce(fmatch_id, '0') = '0' and
                                         coalesce(fmatch_cfg_id, fmatch_log_id, 0) = 0 and fsubname
                                     rlike '1[234]$' and fgamecoins < 0 then
                                     finning_id
                                    else
                                     null
                                  end)) as basic_lose_innings,
                   sum(case
                         when coalesce(fmatch_id, '0') = '0' and
                              coalesce(fmatch_cfg_id, fmatch_log_id, 0) = 0 and fsubname
                          rlike '100$' and
                              unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                              10800 then
                          unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                         else
                          0
                       end) as private_duration,
                   count(distinct(case
                                    when coalesce(fmatch_id, '0') = '0' and
                                         coalesce(fmatch_cfg_id, fmatch_log_id, 0) = 0 and fsubname
                                     rlike '100$' and fgamecoins > 0 then
                                     finning_id
                                    else
                                     null
                                  end)) as private_win_innings,
                   count(distinct(case
                                    when coalesce(fmatch_id, '0') = '0' and
                                         coalesce(fmatch_cfg_id, fmatch_log_id, 0) = 0 and fsubname
                                     rlike '100$' and fgamecoins < 0 then
                                     finning_id
                                    else
                                     null
                                  end)) as private_lose_innings,
                   sum(case
                         when (fmatch_id != '0' or
                              coalesce(fmatch_cfg_id, fmatch_log_id) != 0) and
                              unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                              10800 then
                          unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                         else
                          0
                       end) as match_duration,
                   count(distinct(case
                                    when (fmatch_id != '0' or
                                         coalesce(fmatch_cfg_id, fmatch_log_id) != 0) then
                                     concat(fmatch_id, fmatch_cfg_id, fmatch_log_id)
                                    else
                                     null
                                  end)) as match_count,
                   concat_ws(',', collect_set(fhallname)) as game_halls,
                   concat_ws(',',
                             collect_set(case
                                           when fsubname rlike '12$' then
                                            '初级场'
                                           when fsubname rlike '13$' then
                                            '中级场'
                                           when fsubname rlike '14$' then
                                            '高级场'
                                           when fsubname rlike '100$' then
                                            '私人场'
                                           else
                                            fsubname
                                         end)) as subnames,
                   concat_ws(',', collect_set(fgsubname)) as gsubnames
              from dim.bpid_map t1
             inner join stage.user_gameparty_stg t2
                on t1.fbpid = t2.fbpid
             where t1.fgamename in ('地方棋牌', '德州扑克')
               and dt = '%(statdate)s'
             group by t1.fgamefsk,
                      t1.fgamename,
                      t1.fplatformfsk,
                      t1.fplatformname,
                      t2.fgame_id,
                      t2.fpname,
                      t2.fuid
        """
        res = self.sql_exe(HQL_gameparty_info)
        if res != 0:
            return res

        HQL_match_info = """
            drop table if exists work.match_info_temp_%(statdatenum)s ;
            create table if not exists work.match_info_temp_%(statdatenum)s as
            with match_entry_info as
             (select t1.fgamefsk,
                     t1.fplatformfsk,
                     t2.fgame_id,
                     t2.fuid,
                     sum(case
                           when fgamename = '地方棋牌' and fitem_id = '1' then
                            fentry_fee
                           when fgamename = '德州扑克' then
                            fentry_fee
                           else
                            0
                         end) as match_entry_fee
                from dim.bpid_map t1
               inner join stage.join_gameparty_stg t2
                  on t1.fbpid = t2.fbpid
               where t1.fgamename in ('地方棋牌', '德州扑克')
                 and dt = '%(statdate)s'
               group by t1.fgamefsk, t1.fplatformfsk, t2.fgame_id, t2.fuid),
            match_reward_info as
             (select t1.fgamefsk,
                     t1.fplatformfsk,
                     t2.fgame_id,
                     t2.fuid,
                     count(distinct flts_at) as match_reward_count,
                     sum(case
                           when fgamename = '地方棋牌' and fitem_id = '0' then
                            fitem_num
                           when fgamename = '德州扑克' and fitem_id rlike '游戏币' then
                            fitem_num
                           else
                            0
                         end) as match_reward_gamecoin,
                     sum(case
                           when fgamename = '地方棋牌' and fitem_id = '1' then
                            fitem_num
                           else
                            0
                         end) as match_reward_gold,
                     sum(fcost) as match_reward_cash_value
                from dim.bpid_map t1
               inner join stage.match_economy_stg t2
                  on t1.fbpid = t2.fbpid
               where t1.fgamename in ('地方棋牌', '德州扑克')
                 and dt = '%(statdate)s'
               group by t1.fgamefsk, t1.fplatformfsk, t2.fgame_id, t2.fuid)
            select coalesce(v1.fgamefsk, v2.fgamefsk) as fgamefsk,
                   coalesce(v1.fplatformfsk, v2.fplatformfsk) as fplatformfsk,
                   coalesce(v1.fgame_id, v2.fgame_id) as fgame_id,
                   coalesce(v1.fuid, v2.fuid) as fuid,
                   match_entry_fee,
                   match_reward_count,
                   match_reward_gamecoin,
                   match_reward_gold,
                   match_reward_cash_value
              from match_entry_info v1
              full outer join match_reward_info v2
                on v1.fgamefsk = v2.fgamefsk
               and v1.fplatformfsk = v2.fplatformfsk
               and v1.fgame_id = v2.fgame_id
               and v1.fuid=v2.fuid
        """
        res = self.sql_exe(HQL_match_info)
        if res != 0:
            return res

        HQL_bankrupt_info = """
            drop table if exists work.bankrupt_info_temp_%(statdatenum)s ;
            create table if not exists work.bankrupt_info_temp_%(statdatenum)s as
            with bankrupt_info as
             (select t1.fgamefsk,
                     t1.fplatformfsk,
                     t2.fgame_id,
                     t2.fuid,
                     count(distinct t2.frupt_at) as bankrupt_count
                from dim.bpid_map t1
               inner join stage.user_bankrupt_stg t2
                  on t1.fbpid = t2.fbpid
               where t1.fgamename in ('地方棋牌', '德州扑克')
                 and dt = '%(statdate)s'
               group by t1.fgamefsk, t1.fplatformfsk, t2.fgame_id, t2.fuid),
            relieve_info as
             (select t1.fgamefsk,
                     t1.fplatformfsk,
                     t2.fgame_id,
                     t2.fuid,
                     count(distinct t2.flts_at) as relieve_count,
                     sum(fgamecoins) as relieve_gamecoin
                from dim.bpid_map t1
               inner join stage.user_bankrupt_relieve_stg t2
                  on t1.fbpid = t2.fbpid
               where t1.fgamename in ('地方棋牌', '德州扑克')
                 and dt = '%(statdate)s'
               group by t1.fgamefsk, t1.fplatformfsk, t2.fgame_id, t2.fuid)
            select coalesce(v1.fgamefsk, v2.fgamefsk) as fgamefsk,
                   coalesce(v1.fplatformfsk, v2.fplatformfsk) as fplatformfsk,
                   coalesce(v1.fgame_id, v2.fgame_id) as fgame_id,
                   coalesce(v1.fuid, v2.fuid) as fuid,
                   bankrupt_count,
                   relieve_count,
                   relieve_gamecoin
              from bankrupt_info v1
              full outer join relieve_info v2
                on v1.fgamefsk = v2.fgamefsk
               and v1.fplatformfsk = v2.fplatformfsk
               and v1.fgame_id = v2.fgame_id
               and v1.fuid = v2.fuid
        """
        res = self.sql_exe(HQL_bankrupt_info)
        if res != 0:
            return res

        HQL_item_info = """
            drop table if exists work.item_info_temp_%(statdatenum)s ;
            create table if not exists work.item_info_temp_%(statdatenum)s as
            with item_info as
             (select t1.fgamefsk,
                     t1.fplatformfsk,
                     t2.fgame_id,
                     t2.fuid,
                     count(distinct t2.lts_at) as item_use_count
                from dim.bpid_map t1
               inner join stage.pb_props_stream_stg t2
                  on t1.fbpid = t2.fbpid
               where t1.fgamename in ('地方棋牌', '德州扑克')
                 and dt = '%(statdate)s'
               group by t1.fgamefsk, t1.fplatformfsk, t2.fgame_id, t2.fuid),
            click_info as
             (select t1.fgamefsk,
                     t1.fplatformfsk,
                     t2.fgame_id,
                     t2.fuid,
                     count(distinct t2.flts_at) as click_count
                from dim.bpid_map t1
               inner join stage.click_event_stg t2
                  on t1.fbpid = t2.fbpid
               where t1.fgamename in ('地方棋牌', '德州扑克')
                 and dt = '%(statdate)s'
               group by t1.fgamefsk, t1.fplatformfsk, t2.fgame_id, t2.fuid)
            select coalesce(v1.fgamefsk, v2.fgamefsk) as fgamefsk,
                   coalesce(v1.fplatformfsk, v2.fplatformfsk) as fplatformfsk,
                   coalesce(v1.fgame_id, v2.fgame_id) as fgame_id,
                   coalesce(v1.fuid, v2.fuid) as fuid,
                   item_use_count,
                   click_count
              from item_info v1
              full outer join click_info v2
                on v1.fgamefsk = v2.fgamefsk
               and v1.fplatformfsk = v2.fplatformfsk
               and v1.fgame_id = v2.fgame_id
               and v1.fuid = v2.fuid
        """
        res = self.sql_exe(HQL_item_info)
        if res != 0:
            return res

        HQL_major_info = """
            drop table if exists work.major_info_temp_%(statdatenum)s ;
            create table if not exists work.major_info_temp_%(statdatenum)s as
            select fgamefsk,
                   fplatformfsk,
                   fgame_id,
                   fuid,
                   max(case
                         when fhallname_ranking = 1 then
                          fhallname
                         else
                          null
                       end) as major_hall,
                   max(case
                         when fsubname_ranking = 1 then
                          fsubname
                         else
                          null
                       end) as major_subname,
                   max(case
                         when fsubname_ranking = 1 then
                          round(fsubname_duration / play_duration, 4)
                         else
                          null
                       end) as major_subname_proportion,
                   max(case
                         when fgsubname_ranking = 1 then
                          fgsubname
                         else
                          null
                       end) as major_gsubname,
                   max(case
                         when fgsubname_ranking = 1 then
                          round(fgsubname_duration / play_duration, 4)
                         else
                          null
                       end) as major_gsubname_proportion
              from (select fgamefsk,
                           fplatformfsk,
                           fgame_id,
                           fuid,
                           play_duration,
                           fhallname,
                           row_number() over(partition by fgamefsk, fplatformfsk, fgame_id, fuid, fhallname order by fhallname_duration desc) as fhallname_ranking,
                           fsubname,
                           fsubname_duration,
                           row_number() over(partition by fgamefsk, fplatformfsk, fgame_id, fuid, fsubname order by fsubname_duration desc) as fsubname_ranking,
                           fgsubname,
                           fgsubname_duration,
                           row_number() over(partition by fgamefsk, fplatformfsk, fgame_id, fuid, fgsubname order by fgsubname_duration desc) as fgsubname_ranking
                      from (select distinct t1.fgamefsk,
                                            t1.fplatformfsk,
                                            t2.fgame_id,
                                            t2.fuid,
                                            sum(case
                                                  when unix_timestamp(fe_timer) -
                                                       unix_timestamp(fs_timer) between 1 and
                                                       10800 then
                                                   unix_timestamp(fe_timer) -
                                                   unix_timestamp(fs_timer)
                                                  else
                                                   0
                                                end) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fgame_id, t2.fuid) as play_duration,
                                            t1.fhallname,
                                            sum(case
                                                  when unix_timestamp(fe_timer) -
                                                       unix_timestamp(fs_timer) between 1 and
                                                       10800 then
                                                   unix_timestamp(fe_timer) -
                                                   unix_timestamp(fs_timer)
                                                  else
                                                   0
                                                end) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fgame_id, t2.fuid, t1.fhallname) as fhallname_duration,
                                            t2.fsubname,
                                            sum(case
                                                  when unix_timestamp(fe_timer) -
                                                       unix_timestamp(fs_timer) between 1 and
                                                       10800 then
                                                   unix_timestamp(fe_timer) -
                                                   unix_timestamp(fs_timer)
                                                  else
                                                   0
                                                end) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fgame_id, t2.fuid, t2.fsubname) as fsubname_duration,
                                            t2.fgsubname,
                                            sum(case
                                                  when unix_timestamp(fe_timer) -
                                                       unix_timestamp(fs_timer) between 1 and
                                                       10800 then
                                                   unix_timestamp(fe_timer) -
                                                   unix_timestamp(fs_timer)
                                                  else
                                                   0
                                                end) over(partition by t1.fgamefsk, t1.fplatformfsk, t2.fgame_id, t2.fuid, t2.fgsubname) as fgsubname_duration
                              from dim.bpid_map t1
                             inner join stage.user_gameparty_stg t2
                                on t1.fbpid = t2.fbpid
                             where t1.fgamename in ('地方棋牌', '德州扑克')
                               and dt = '%(statdate)s') m1) m2
             where fhallname_ranking = 1
                or fsubname_ranking = 1
                or fgsubname_ranking = 1
             group by fgamefsk, fplatformfsk, fgame_id, fuid
        """
        res = self.sql_exe(HQL_major_info)
        if res != 0:
            return res

        HQL = """
            insert overwrite table veda.subgame_user_action_everyday partition (dt = '%(statdate)s')
            select v1.fgamefsk,
                   v1.fgamename,
                   v1.fplatformfsk,
                   v1.fplatformname,
                   v1.fgame_id,
                   coalesce(v1.fpname, v2.fpname) as fpname,
                   v1.fuid,
                   v2.enter_count,
                   v1.game_duration,
                   v1.game_innings,
                   v1.win_gamecoin,
                   v1.lose_gamecoin,
                   v1.table_charge_gamecoin,
                   v1.net_gamecoin,
                   v1.win_gold,
                   v1.lose_gold,
                   v1.table_charge_gold,
                   v1.net_gold,
                   v4.bankrupt_count,
                   v4.relieve_count,
                   v4.relieve_gamecoin,
                   v1.trustee_count,
                   v5.item_use_count,
                   v5.click_count,
                   v1.banker_count,
                   v1.discard_count,
                   v1.show_card_count,
                   v1.stand_count,
                   v1.abnormal_end_count,
                   v1.basic_duration,
                   v1.basic_win_innings,
                   v1.basic_lose_innings,
                   v1.private_duration,
                   v1.private_win_innings,
                   v1.private_lose_innings,
                   v1.match_duration,
                   v1.match_count,
                   v3.match_entry_fee,
                   v3.match_reward_count,
                   v3.match_reward_gamecoin,
                   v3.match_reward_gold,
                   v3.match_reward_cash_value,
                   v1.game_halls,
                   v6.major_hall,
                   v1.subnames,
                   v6.major_subname,
                   v6.major_subname_proportion,
                   v1.gsubnames,
                   v6.major_gsubname,
                   v6.major_gsubname_proportion,
                   v2.latest_game_version
              from work.gameparty_info_temp_%(statdatenum)s v1
              left join work.enter_info_temp_%(statdatenum)s v2
                on (v1.fgamefsk = v2.fgamefsk and v1.fplatformfsk = v2.fplatformfsk and v1.fgame_id = v2.fgame_id and v1.fuid = v2.fuid)
              left join work.match_info_temp_%(statdatenum)s v3
                on (v1.fgamefsk = v3.fgamefsk and v1.fplatformfsk = v3.fplatformfsk and v1.fgame_id = v3.fgame_id and v1.fuid = v3.fuid)
              left join work.bankrupt_info_temp_%(statdatenum)s v4
                on (v1.fgamefsk = v4.fgamefsk and v1.fplatformfsk = v4.fplatformfsk and v1.fgame_id = v4.fgame_id and v1.fuid = v4.fuid)
              left join work.item_info_temp_%(statdatenum)s v5
                on (v1.fgamefsk = v5.fgamefsk and v1.fplatformfsk = v5.fplatformfsk and v1.fgame_id = v5.fgame_id and v1.fuid = v5.fuid)
              left join work.major_info_temp_%(statdatenum)s v6
                on (v1.fgamefsk = v6.fgamefsk and v1.fplatformfsk = v6.fplatformfsk and v1.fgame_id = v6.fgame_id and v1.fuid = v6.fuid)
            distribute by v1.fgamefsk, v1.fplatformfsk, v1.fgame_id
              sort by v1.fuid desc
        """
        res = self.sql_exe(HQL)
        if res != 0:
            return res

        HQL_drop_temp_table = """
            drop table if exists work.enter_info_temp_%(statdatenum)s ;
            drop table if exists work.gameparty_info_temp_%(statdatenum)s ;
            drop table if exists work.match_info_temp_%(statdatenum)s ;
            drop table if exists work.bankrupt_info_temp_%(statdatenum)s ;
            drop table if exists work.item_info_temp_%(statdatenum)s ;
            drop table if exists work.major_info_temp_%(statdatenum)s ;
            drop table if exists work.enter_info_temp_%(statdatenum)s ;
        """
        res = self.sql_exe(HQL_drop_temp_table)
        if res != 0:
            return res

        return res


# 实例化，运行
a = SubgameUserActionEveryday(sys.argv[1:])
a()
