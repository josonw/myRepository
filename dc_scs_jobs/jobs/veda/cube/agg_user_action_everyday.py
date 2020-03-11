#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserActionEveryday(BaseStatModel):

    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS veda.user_action_everyday
            (
              fgamefsk bigint COMMENT '游戏ID',
              fgamename string COMMENT '游戏名称',
              fplatformfsk bigint COMMENT '平台ID',
              fplatformname string COMMENT '平台名称',
              fhallfsk bigint COMMENT '大厅ID',
              fhallname string COMMENT '大厅名称',
              fterminaltypefsk bigint COMMENT '终端ID',
              fterminaltypename string COMMENT '终端名称',
              fversionfsk bigint COMMENT '版本ID',
              fversionname string COMMENT '版本名称',
              fuid bigint COMMENT '用户ID',
              login_count INT COMMENT '登录次数',
              enter_count INT COMMENT '进入游戏次数',
              game_duration DECIMAL(7,1) COMMENT '游戏时长(分钟)',
              game_inngings INT COMMENT '玩牌局数',
              win_innings INT COMMENT '胜局数',
              lose_innings INT COMMENT '输局数',
              win_gamecoin BIGINT COMMENT '赢取游戏币',
              lose_gamecoin BIGINT COMMENT '输去游戏币',
              net_gamecoin BIGINT COMMENT '游戏币净输赢',
              win_gold BIGINT COMMENT '赢取金条',
              lose_gold BIGINT COMMENT '输去金条',
              net_gold BIGINT COMMENT '金条净输赢',
              table_charge_gamecoin BIGINT COMMENT '台费游戏币',
              table_charge_gold BIGINT COMMENT '台费金条',
              basic_duration DECIMAL(7,1) COMMENT '普通场玩牌时长(分钟,不含比赛)',
              basic_innings INT COMMENT '普通场玩牌局数(不含比赛)',
              match_duration DECIMAL(7,1) COMMENT '比赛场时长(分钟)',
              match_entry_count INT COMMENT '比赛报名次数',
              match_count INT COMMENT '实际比赛次数',
              match_innings INT COMMENT '比赛局数',
              match_entry_fee BIGINT COMMENT '报名费游戏币(地方棋牌为金条)',
              match_reward_count INT COMMENT '比赛获奖次数',
              match_reward_gamecoin BIGINT COMMENT '比赛获奖游戏币',
              match_reward_gold BIGINT COMMENT '比赛获奖金条',
              match_reward_cash_value DECIMAL(10,2) COMMENT '比赛获奖现金价值(人民币元)',
              pay_count INT COMMENT '付费次数',
              pay_sum DECIMAL(10,2) COMMENT '付费额度(美元)',
              bankrupt_count INT COMMENT '破产次数',
              relieve_count INT COMMENT '领取救济次数',
              relieve_gamecoin BIGINT COMMENT '领取救济游戏币',
              click_count INT COMMENT '点击次数',
              subgame_count INT COMMENT '玩子游戏数量',
              major_subgame STRING COMMENT '主玩子游戏(当天玩时间最长的游戏)',
              major_subgame_proportion DECIMAL(5,4) COMMENT '主玩子游戏时长占比'
            )
            COMMENT '用户每天行为统计'
            PARTITIONED BY (dt STRING COMMENT '日期')
            STORED AS PARQUET
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            with login_info as
             (select fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname,
                     fuid,
                     login_count
                from (select fgamefsk,
                             fgamename,
                             fplatformfsk,
                             fplatformname,
                             fhallfsk,
                             fhallname,
                             fterminaltypefsk,
                             fterminaltypename,
                             fversionfsk,
                             fversionname,
                             fuid,
                             count(distinct flogin_at) over(partition by fgamefsk, fplatformfsk, fuid) as login_count,
                             row_number() over(partition by fgamefsk, fplatformfsk, fuid order by flogin_at desc) as ranking
                        from dim.bpid_map t1
                       inner join stage.user_login_stg t2
                          on t1.fbpid = t2.fbpid
                       where t1.fgamename in ('地方棋牌', '德州扑克')
                         and dt = '%(statdate)s') m1
               where ranking = 1),
            enter_info as
             (select fgamefsk,
                     fplatformfsk,
                     fuid,
                     count(distinct flts_at) as enter_count
                from dim.bpid_map t1
               inner join stage.user_enter_stg t2
                  on t1.fbpid = t2.fbpid
               where t1.fgamename in ('地方棋牌', '德州扑克')
                 and dt = '%(statdate)s'
               group by fgamefsk, fplatformfsk, fuid),
            gameparty_info as
             (select fgamefsk,
                     fplatformfsk,
                     fuid,
                     sum(case
                           when unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                10800 then
                            unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                           else
                            0
                         end) as game_duration,
                     count(distinct finning_id) as game_innings,
                     count(distinct(case
                                      when fgamecoins > 0 then
                                       finning_id
                                      else
                                       null
                                    end)) as win_innings,
                     count(distinct(case
                                      when fgamecoins < 0 then
                                       finning_id
                                      else
                                       null
                                    end)) as lose_innings,
                     sum(case
                           when (fgamename = '地方棋牌' and coalesce(fcoins_type, 1) = 1 or fgamename = '德州扑克' and coalesce(fcoins_type, 0) = 0) and fgamecoins > 0 then
                            fgamecoins
                           else
                            0
                         end) as win_gamecoin,
                     sum(case
                           when (fgamename = '地方棋牌' and coalesce(fcoins_type, 1) = 1 or fgamename = '德州扑克' and coalesce(fcoins_type, 0) = 0) and fgamecoins < 0 then
                            abs(fgamecoins)
                           else
                            0
                         end) as lose_gamecoin,
                     sum(case
                           when fgamename = '地方棋牌' and coalesce(fcoins_type, 1) = 1 or fgamename = '德州扑克' and coalesce(fcoins_type, 0) = 0 then
                            fgamecoins
                           else
                            0
                         end) as net_gamecoin,
                     sum(case
                           when fcoins_type = 11 and fgamecoins > 0 then
                            fgamecoins
                           else
                            0
                         end) as win_gold,
                     sum(case
                           when fcoins_type = 11 and fgamecoins < 0 then
                            abs(fgamecoins)
                           else
                            0
                         end) as lose_gold,
                     sum(case
                           when fcoins_type = 11 then
                            fgamecoins
                           else
                            0
                         end) as net_gold,
                     sum(case
                           when fgamename = '地方棋牌' and coalesce(fcoins_type, 1) = 1 or fgamename = '德州扑克' and coalesce(fcoins_type, 0) = 0 then
                            fcharge
                           else
                            0
                         end) as table_charge_gamecoin,
                     sum(case
                           when fcoins_type = 11 then
                            fcharge
                           else
                            0
                         end) as table_charge_gold,
                     sum(case
                           when coalesce(fmatch_id, '0') = '0' and
                                coalesce(fmatch_cfg_id, fmatch_log_id, 0) = 0 and
                                unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                10800 then
                            unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                           else
                            0
                         end) as basic_duration,
                     count(distinct(case
                                      when coalesce(fmatch_id, '0') = '0' and
                                           coalesce(fmatch_cfg_id, fmatch_log_id, 0) = 0 then
                                       finning_id
                                      else
                                       null
                                    end)) as basic_innings,
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
                     count(distinct(case
                                      when (fmatch_id != '0' or
                                           coalesce(fmatch_cfg_id, fmatch_log_id) != 0) then
                                       finning_id
                                      else
                                       null
                                    end)) as match_innings,
                     count(distinct fpname) as subgame_count
                from dim.bpid_map t1
               inner join stage.user_gameparty_stg t2
                  on t1.fbpid = t2.fbpid
               where t1.fgamename in ('地方棋牌', '德州扑克')
                 and dt = '%(statdate)s'
               group by fgamefsk, fplatformfsk, fuid),
            match_entry_info as
             (select fgamefsk,
                     fplatformfsk,
                     fuid,
                     count(distinct flts_at) as match_entry_count,
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
               group by fgamefsk, fplatformfsk, fuid),
            match_reward_info as
             (select fgamefsk,
                     fplatformfsk,
                     fuid,
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
               group by fgamefsk, fplatformfsk, fuid),
            payment_info as
             (select fgamefsk,
                     fplatformfsk,
                     fuid,
                     count(distinct fdate) as pay_count,
                     sum(fpamount_usd) as pay_sum
                from dim.bpid_map t1
               inner join stage.payment_stream_stg t2
                  on t1.fbpid = t2.fbpid
               where t1.fgamename in ('地方棋牌', '德州扑克')
                 and dt = '%(statdate)s'
               group by fgamefsk, fplatformfsk, fuid),
            bankrupt_info as
             (select fgamefsk,
                     fplatformfsk,
                     fuid,
                     count(distinct frupt_at) as bankrupt_count
                from dim.bpid_map t1
               inner join stage.user_bankrupt_stg t2
                  on t1.fbpid = t2.fbpid
               where t1.fgamename in ('地方棋牌', '德州扑克')
                 and dt = '%(statdate)s'
               group by fgamefsk, fplatformfsk, fuid),
            relieve_info as
             (select fgamefsk,
                     fplatformfsk,
                     fuid,
                     count(distinct flts_at) as relieve_count,
                     sum(fgamecoins) as relieve_gamecoin
                from dim.bpid_map t1
               inner join stage.user_bankrupt_relieve_stg t2
                  on t1.fbpid = t2.fbpid
               where t1.fgamename in ('地方棋牌', '德州扑克')
                 and dt = '%(statdate)s'
               group by fgamefsk, fplatformfsk, fuid),
            click_info as
             (select fgamefsk,
                     fplatformfsk,
                     fuid,
                     count(1) as click_count
                from dim.bpid_map t1
               inner join stage.click_event_stg t2
               where t1.fgamename in ('地方棋牌', '德州扑克')
                 and dt = '%(statdate)s'
               group by fgamefsk, fplatformfsk, fuid),
            major_subgame_info as
             (select fgamefsk,
                     fplatformfsk,
                     fuid,
                     major_subgame,
                     major_subgame_duration,
                     row_number() over(partition by fuid order by major_subgame_duration desc) as ranking
                from (select fgamefsk,
                             fplatformfsk,
                             fuid,
                             fpname as major_subgame,
                             sum(case
                                   when unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                        10800 then
                                    unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                                   else
                                    0
                                 end) as major_subgame_duration
                        from dim.bpid_map t1
                       inner join stage.user_gameparty_stg t2
                          on t1.fbpid = t2.fbpid
                       where t1.fgamename in ('地方棋牌', '德州扑克')
                         and dt = '%(statdate)s'
                       group by fgamefsk, fplatformfsk, fuid, fpname) m2)
            insert overwrite table veda.user_action_everyday partition (dt = '%(statdate)s')
            select v1.fgamefsk,
                   v1.fgamename,
                   v1.fplatformfsk,
                   v1.fplatformname,
                   v1.fhallfsk,
                   v1.fhallname,
                   v1.fterminaltypefsk,
                   v1.fterminaltypename,
                   v1.fversionfsk,
                   v1.fversionname,
                   v1.fuid,
                   v1.login_count,
                   coalesce(v2.enter_count, 0) as enter_count,
                   coalesce(round(v3.game_duration / 60, 1), 0) as game_duration,
                   coalesce(v3.game_innings, 0) as game_innings,
                   coalesce(v3.win_innings, 0) as win_innings,
                   coalesce(v3.lose_innings, 0) as lose_innings,
                   coalesce(v3.win_gamecoin, 0) as win_gamecoin,
                   coalesce(v3.lose_gamecoin, 0) as lose_gamecoin,
                   coalesce(v3.net_gamecoin, 0) as net_gamecoin,
                   coalesce(v3.win_gold, 0) as win_gold,
                   coalesce(v3.lose_gold, 0) as lose_gold,
                   coalesce(v3.net_gold, 0) as net_gold,
                   coalesce(v3.table_charge_gamecoin, 0) as table_charge_gamecoin,
                   coalesce(v3.table_charge_gold, 0) as table_charge_gold,
                   coalesce(round(v3.basic_duration / 60, 1), 0) as basic_duration,
                   coalesce(v3.basic_innings, 0) as basic_innings,
                   coalesce(round(v3.match_duration / 60, 1), 0) as match_duration,
                   coalesce(v4.match_entry_count, 0) as match_entry_count,
                   coalesce(v3.match_count, 0) as match_count,
                   coalesce(v3.match_innings, 0) as match_innings,
                   coalesce(v4.match_entry_fee, 0) as match_entry_fee,
                   coalesce(v5.match_reward_count, 0) as match_reward_count,
                   coalesce(v5.match_reward_gamecoin, 0) as match_reward_gamecoin,
                   coalesce(v5.match_reward_gold, 0) as match_reward_gold,
                   coalesce(v5.match_reward_cash_value, 0) as match_reward_cash_value,
                   coalesce(v6.pay_count, 0) as pay_count,
                   coalesce(v6.pay_sum, 0) as pay_sum,
                   coalesce(v7.bankrupt_count, 0) as bankrupt_count,
                   coalesce(v8.relieve_count, 0) as relieve_count,
                   coalesce(v8.relieve_gamecoin, 0) as relieve_gamecoin,
                   coalesce(v10.click_count, 0) as click_count,
                   coalesce(v3.subgame_count, 0) as subgame_count,
                   v9.major_subgame,
                   coalesce(cast(v9.major_subgame_duration / v3.game_duration as
                                 decimal(5, 4)),
                            0) as major_subgame_proportion
              from login_info v1
              left join enter_info v2
                on (v1.fgamefsk = v2.fgamefsk and v1.fplatformfsk = v2.fplatformfsk and
                   v1.fuid = v2.fuid)
              left join gameparty_info v3
                on (v1.fgamefsk = v3.fgamefsk and v1.fplatformfsk = v3.fplatformfsk and
                   v1.fuid = v3.fuid)
              left join match_entry_info v4
                on (v1.fgamefsk = v4.fgamefsk and v1.fplatformfsk = v4.fplatformfsk and
                   v1.fuid = v4.fuid)
              left join match_reward_info v5
                on (v1.fgamefsk = v5.fgamefsk and v1.fplatformfsk = v5.fplatformfsk and
                   v1.fuid = v5.fuid)
              left join payment_info v6
                on (v1.fgamefsk = v6.fgamefsk and v1.fplatformfsk = v6.fplatformfsk and
                   v1.fuid = v6.fuid)
              left join bankrupt_info v7
                on (v1.fgamefsk = v7.fgamefsk and v1.fplatformfsk = v7.fplatformfsk and
                   v1.fuid = v7.fuid)
              left join relieve_info v8
                on (v1.fgamefsk = v8.fgamefsk and v1.fplatformfsk = v8.fplatformfsk and
                   v1.fuid = v8.fuid)
              left join major_subgame_info v9
                on (v1.fgamefsk = v9.fgamefsk and v1.fplatformfsk = v9.fplatformfsk and
                   v1.fuid = v9.fuid and v9.ranking = 1)
              left join click_info v10
                on (v1.fgamefsk = v10.fgamefsk and v1.fplatformfsk = v10.fplatformfsk and
                   v1.fuid = v10.fuid)
            distribute by v1.fgamefsk, v1.fplatformfsk
              sort by v1.fuid
             limit 100000000
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化，运行
a = UserActionEveryday(sys.argv[1:])
a()