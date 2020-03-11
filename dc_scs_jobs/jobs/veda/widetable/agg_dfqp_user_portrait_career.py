#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserPortraitCareerInfo(BaseStatModel):

    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS veda.dfqp_user_portrait_career(
              mid bigint COMMENT 'MID',
              login_days int COMMENT '登录天数',
              play_days int COMMENT '玩牌天数',
              bankrupt_days int COMMENT '破产天数',
              relieve_days int COMMENT '领取救济天数',
              match_days int COMMENT '比赛天数',
              login_count int COMMENT '登录次数',
              total_innings int COMMENT '玩牌局数',
              win_innings int COMMENT '胜局数',
              lose_innings int COMMENT '输局数',
              play_duration int COMMENT '玩牌时长(秒)',
              pay_count int COMMENT '付费次数',
              pay_sum decimal(10,2) COMMENT '付费总额',
              match_enroll_count int COMMENT '比赛报名次数',
              match_innings int COMMENT '比赛局数',
              match_duration int COMMENT '比赛时长(秒)',
              match_win_innings int COMMENT '比赛获胜局数',
              bankrupt_count int COMMENT '破产次数',
              relieve_count int COMMENT '领取救济次数',
              relieve_silver_coins bigint COMMENT '领取救济总额',
              latest_login_time string COMMENT '最后登录时间',
              latest_active_time string COMMENT '最后活跃时间',
              latest_play_time string COMMENT '最后玩牌时间',
              latest_pay_time string COMMENT '最后付费时间',
              latest_pay_money decimal(10,2) COMMENT '最后付费金额',
              latest_match_time string COMMENT '最后比赛时间',
              latest_bankrupt_time string COMMENT '最后破产时间',
              latest_relieve_time string COMMENT '最后领取救济时间',
              first_login_time string COMMENT '首次登录时间',
              first_play_time string COMMENT '首次玩牌时间',
              first_pay_time string COMMENT '首次付费时间',
              first_match_time string COMMENT '首次比赛时间',
              first_bankrupt_time string COMMENT '首次破产时间',
              first_relieve_time string COMMENT '首次领取救济时间',
              last_2nd_login_time string COMMENT '最后登录上一次时间',
              last_2nd_active_time string COMMENT '最后活跃上一次时间',
              last_2nd_play_time string COMMENT '最后玩牌上一次时间',
              last_2nd_pay_time string COMMENT '最后付费上一次时间',
              last_2nd_match_time string COMMENT '最后比赛上一次时间',
              latest_device_imei string COMMENT '最后登录设备IMEI',
              latest_device_imsi string COMMENT '最后登录设备IMSI',
              latest_device_type string COMMENT '最后登录设备型号',
              latest_device_pixel string COMMENT '最后登录设备分辨率',
              latest_device_os string COMMENT '最后登录操作系统',
              latest_app_version string COMMENT '最后登录版本',
              latest_network string COMMENT '最后登录网络类型',
              latest_operator string COMMENT '最后登录运营商',
              latest_login_ip string COMMENT '最后登录IP',
              latest_login_ip_country string COMMENT '最后登录IP所属国家',
              latest_login_ip_province string COMMENT '最后登录IP所属省份',
              latest_login_ip_city string COMMENT '最后登录IP所属城市',
              recent_login_series_days int COMMENT '最近连续登录天数',
              max_login_series_days int COMMENT '最大连续登录天数',
              latest_pname string COMMENT '最后玩牌一次场次',
              latest_subname string COMMENT '最后玩牌二级场次',
              latest_gsubname string COMMENT '最后玩牌三级场次',
              latest_play_coins bigint COMMENT '最后玩牌输赢货币量',
              purchase_silver_money decimal(10,2) COMMENT '付费购买银币金额',
              purchase_gold_money decimal(10,2) COMMENT '付费购买金条金额',
              purchase_vip_money decimal(10,2) COMMENT '付费购买VIP金额',
              purchase_items_money decimal(10,2) COMMENT '付费购买其它物品金额',
              max_pay_money decimal(10,2) COMMENT '最大付费金额',
              match_entry_fee bigint COMMENT '比赛总报名费',
              max_match_entry_fee bigint COMMENT '比赛最大报名费',
              latest_match_pname string COMMENT '最后比赛一次场次',
              latest_match_subname string COMMENT '最后比赛二级场次',
              latest_match_gsubname string COMMENT '最后比赛三级场次',
              match_reward decimal(10,2) COMMENT '比赛获奖总金额',
              max_match_reward decimal(10,2) COMMENT '比赛最大获奖金额',
              silver_innings int COMMENT '银币场玩牌局数',
              silver_duration int COMMENT '银币场玩牌时长(秒)',
              silver_win_innings int COMMENT '银币场胜局数',
              silver_lose_innings int COMMENT '银币场输局数',
              silver_service_charge bigint COMMENT '银币场台费',
              silver_net_coins bigint COMMENT '银币场净输赢银币',
              silver_win_coins bigint COMMENT '银币场赢取银币',
              silver_lose_coins bigint COMMENT '银币场输去银币',
              silver_max_win_coins bigint COMMENT '银币场最大赢取银币',
              silver_max_lose_coins bigint COMMENT '银币场最大输去银币',
              gold_innings int COMMENT '金条场玩牌局数',
              gold_duration int COMMENT '金条场玩牌时长(秒)',
              gold_win_innings int COMMENT '金条场胜局数',
              gold_lose_innings int COMMENT '金条场输局数',
              gold_service_charge bigint COMMENT '金条场台费',
              gold_net_coins bigint COMMENT '金条场净输赢金条',
              gold_win_coins bigint COMMENT '金条场赢取金条',
              gold_lose_coins bigint COMMENT '金条场输去金条',
              gold_max_win_coins bigint COMMENT '金条场最大赢取金条',
              gold_max_lose_coins bigint COMMENT '金条场最大输去金条',
              score_innings int COMMENT '积分场玩牌局数',
              score_duration int COMMENT '积分场玩牌时长(秒)',
              score_win_innings int COMMENT '积分场胜局数',
              score_lose_innings int COMMENT '积分场输局数',
              score_service_charge bigint COMMENT '积分场台费',
              score_net_points bigint COMMENT '积分场净输赢积分',
              score_win_points bigint COMMENT '积分场赢取积分',
              score_lose_points bigint COMMENT '积分场输去积分',
              score_max_win_points bigint COMMENT '积分场最大赢取积分',
              score_max_lose_points bigint COMMENT '积分场最大输去积分',
              if_signup_2days char(1) COMMENT '是否已注册2天',
              if_signup_3days char(1) COMMENT '是否已注册3天',
              if_signup_7days char(1) COMMENT '是否已注册7天',
              if_signup_15days char(1) COMMENT '是否已注册15天',
              if_signup_30days char(1) COMMENT '是否已注册30天',
              if_signup_60days char(1) COMMENT '是否已注册60天',
              if_signup_90days char(1) COMMENT '是否已注册90天',
              if_away_7days char(1) COMMENT '是否已流失7天',
              if_away_15days char(1) COMMENT '是否已流失15天',
              if_away_30days char(1) COMMENT '是否已流失30天',
              if_away_60days char(1) COMMENT '是否已流失60天',
              if_away_90days char(1) COMMENT '是否已流失90天')
            COMMENT '地方棋牌用户生涯信息'
            STORED AS ORC
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
    	  exec_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    	  if len(sys.argv) == 1 or exec_date == sys.argv[1]:

            ############  将当前数据备份到历史表  ############
            hql = """
                insert overwrite table veda.dfqp_user_portrait_career_history partition (dt)
                select t.*, date_sub('%(statdate)s',1) as dt
                from veda.dfqp_user_portrait_career t
            """
            res = self.sql_exe(hql)
            if res != 0:
                return res
    
            ############  统计当天数据更新画像表  ############
            hql = """
                --登录信息
                with login_info as
                 (select mid,
                         login_count,
                         latest_login_time,
                         latest_device_imei,
                         latest_device_imsi,
                         latest_device_type,
                         latest_device_pixel,
                         latest_device_os,
                         latest_app_version,
                         latest_network,
                         latest_operator,
                         latest_login_ip,
                         latest_login_ip_country,
                         latest_login_ip_province,
                         latest_login_ip_city
                    from (select fuid as mid,
                                 count(distinct flogin_at) over(partition by fuid) as login_count,
                                 flogin_at as latest_login_time,
                                 fm_imei as latest_device_imei,
                                 fm_imsi as latest_device_imsi,
                                 fm_dtype as latest_device_type,
                                 fm_pixel as latest_device_pixel,
                                 fm_os as latest_device_os,
                                 fversion_info as latest_app_version,
                                 fm_network as latest_network,
                                 fm_operator as latest_operator,
                                 fip as latest_login_ip,
                                 fip_country as latest_login_ip_country,
                                 fip_province as latest_login_ip_province,
                                 fip_city as latest_login_ip_city,
                                 row_number() over(partition by fuid order by flogin_at desc) as ranking
                            from stage_dfqp.user_login_stg
                           where dt = '%(statdate)s') m1
                   where ranking = 1),
                
                --牌局信息
                gameparty_info_1 as
                 (select fuid as mid,
                         count(distinct finning_id) as total_innings,
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
                               when unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                    10800 then
                                unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                               else
                                0
                             end) as play_duration,
                         count(distinct(case
                                          when fcoins_type = 1 then
                                           finning_id
                                          else
                                           null
                                        end)) as silver_innings,
                         sum(case
                               when fcoins_type = 1 and
                                    unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                    10800 then
                                unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                               else
                                0
                             end) as silver_duration,
                         count(distinct(case
                                          when fcoins_type = 1 and fgamecoins > 0 then
                                           finning_id
                                          else
                                           null
                                        end)) as silver_win_innings,
                         count(distinct(case
                                          when fcoins_type = 1 and fgamecoins < 0 then
                                           finning_id
                                          else
                                           null
                                        end)) as silver_lose_innings,
                         sum(case
                               when fcoins_type = 1 then
                                fcharge
                               else
                                0
                             end) as silver_service_charge,
                         sum(case
                               when fcoins_type = 1 then
                                fgamecoins
                               else
                                0
                             end) as silver_net_coins,
                         sum(case
                               when fcoins_type = 1 and fgamecoins > 0 then
                                fgamecoins
                               else
                                0
                             end) as silver_win_coins,
                         sum(case
                               when fcoins_type = 1 and fgamecoins < 0 then
                                fgamecoins
                               else
                                0
                             end) as silver_lose_coins,
                         count(distinct(case
                                          when fcoins_type = 11 then
                                           finning_id
                                          else
                                           null
                                        end)) as gold_innings,
                         sum(case
                               when fcoins_type = 11 and
                                    unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                    10800 then
                                unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                               else
                                0
                             end) as gold_duration,
                         count(distinct(case
                                          when fcoins_type = 11 and fgamecoins > 0 then
                                           finning_id
                                          else
                                           null
                                        end)) as gold_win_innings,
                         count(distinct(case
                                          when fcoins_type = 11 and fgamecoins < 0 then
                                           finning_id
                                          else
                                           null
                                        end)) as gold_lose_innings,
                         sum(case
                               when fcoins_type = 11 then
                                fcharge
                               else
                                0
                             end) as gold_service_charge,
                         sum(case
                               when fcoins_type = 11 then
                                fgamecoins
                               else
                                0
                             end) as gold_net_coins,
                         sum(case
                               when fcoins_type = 11 and fgamecoins > 0 then
                                fgamecoins
                               else
                                0
                             end) as gold_win_coins,
                         sum(case
                               when fcoins_type = 11 and fgamecoins < 0 then
                                fgamecoins
                               else
                                0
                             end) as gold_lose_coins,
                         count(distinct(case
                                          when fcoins_type = 3 then
                                           finning_id
                                          else
                                           null
                                        end)) as score_innings,
                         sum(case
                               when fcoins_type = 3 and
                                    unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                    10800 then
                                unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                               else
                                0
                             end) as score_duration,
                         count(distinct(case
                                          when fcoins_type = 3 and fgamecoins > 0 then
                                           finning_id
                                          else
                                           null
                                        end)) as score_win_innings,
                         count(distinct(case
                                          when fcoins_type = 3 and fgamecoins < 0 then
                                           finning_id
                                          else
                                           null
                                        end)) as score_lose_innings,
                         sum(case
                               when fcoins_type = 3 then
                                fcharge
                               else
                                0
                             end) as score_service_charge,
                         sum(case
                               when fcoins_type = 3 then
                                fgamecoins
                               else
                                0
                             end) as score_net_points,
                         sum(case
                               when fcoins_type = 3 and fgamecoins > 0 then
                                fgamecoins
                               else
                                0
                             end) as score_win_points,
                         sum(case
                               when fcoins_type = 3 and fgamecoins < 0 then
                                fgamecoins
                               else
                                0
                             end) as score_lose_points
                    from stage_dfqp.user_gameparty_stg
                   where dt = '%(statdate)s'
                   group by fuid),
                gameparty_info_2 as
                 (select mid,
                         latest_play_time,
                         latest_pname,
                         latest_subname,
                         latest_gsubname,
                         latest_play_coins
                    from (select fuid as mid,
                                 fe_timer as latest_play_time,
                                 fpname as latest_pname,
                                 fsubname as latest_subname,
                                 fgsubname as latest_gsubname,
                                 fgamecoins as latest_play_coins,
                                 row_number() over(partition by fuid order by flts_at desc) as ranking
                            from stage_dfqp.user_gameparty_stg
                           where dt = '%(statdate)s') m2
                   where ranking = 1),
                gameparty_info_all as
                 (select g1.mid,
                         total_innings,
                         win_innings,
                         lose_innings,
                         play_duration,
                         silver_innings,
                         silver_duration,
                         silver_win_innings,
                         silver_lose_innings,
                         silver_service_charge,
                         silver_net_coins,
                         silver_win_coins,
                         silver_lose_coins,
                         gold_innings,
                         gold_duration,
                         gold_win_innings,
                         gold_lose_innings,
                         gold_service_charge,
                         gold_net_coins,
                         gold_win_coins,
                         gold_lose_coins,
                         score_innings,
                         score_duration,
                         score_win_innings,
                         score_lose_innings,
                         score_service_charge,
                         score_net_points,
                         score_win_points,
                         score_lose_points,
                         latest_play_time,
                         latest_pname,
                         latest_subname,
                         latest_gsubname,
                         latest_play_coins
                    from gameparty_info_1 g1
                   inner join gameparty_info_2 g2
                      on g1.mid = g2.mid),
                
                --比赛信息
                match_info as
                 (select mid,
                         match_innings,
                         match_duration,
                         match_win_innings,
                         latest_match_time,
                         latest_match_pname,
                         latest_match_subname,
                         latest_match_gsubname
                    from (select fuid as mid,
                                 count(distinct finning_id) over(partition by fuid) as match_innings,
                                 sum(case
                                       when unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                            10800 then
                                        unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                                       else
                                        0
                                     end) over(partition by fuid) as match_duration,
                                 count(distinct(case
                                                  when fgamecoins > 0 then
                                                   finning_id
                                                  else
                                                   null
                                                end)) over(partition by fuid) as match_win_innings,
                                 flts_at as latest_match_time,
                                 fpname as latest_match_pname,
                                 fsubname as latest_match_subname,
                                 fgsubname as latest_match_gsubname,
                                 row_number() over(partition by fuid order by flts_at desc) as ranking
                            from stage_dfqp.user_gameparty_stg
                           where fmatch_id != '0'
                             and dt = '%(statdate)s') m3
                   where ranking = 1),
                
                --破产信息
                bankrupt_info as
                 (select fuid as mid,
                         count(distinct frupt_at) as bankrupt_count,
                         max(frupt_at) as latest_bankrupt_time
                    from stage_dfqp.user_bankrupt_stg
                   where dt = '%(statdate)s'
                   group by fuid),
                
                --救济领取信息
                relieve_info as
                 (select fuid as mid,
                         count(distinct flts_at) as relieve_count,
                         sum(fgamecoins) as relieve_silver_coins,
                         max(flts_at) as latest_relieve_time
                    from stage_dfqp.user_bankrupt_relieve_stg
                   where dt = '%(statdate)s'
                   group by fuid),
                
                --付费信息
                pay_info as
                 (select mid,
                         pay_count,
                         pay_sum,
                         latest_pay_time,
                         latest_pay_money,
                         purchase_silver_money,
                         purchase_gold_money,
                         purchase_vip_money,
                         purchase_items_money,
                         max_pay_money
                    from (select fuid as mid,
                                 count(fdate) over(partition by fuid) as pay_count,
                                 sum(fcoins_num) over(partition by fuid) as pay_sum,
                                 fdate as latest_pay_time,
                                 fcoins_num as latest_pay_money,
                                 sum(case
                                       when fproduct_name rlike '(金币|银币|銀幣|游戏币)' then
                                        fcoins_num
                                       else
                                        0
                                     end) over(partition by fuid) as purchase_silver_money,
                                 sum(case
                                       when fproduct_name rlike '(金条|金條)' then
                                        fcoins_num
                                       else
                                        0
                                     end) over(partition by fuid) as purchase_gold_money,
                                 sum(case
                                       when fproduct_name rlike 'VIP' then
                                        fcoins_num
                                       else
                                        0
                                     end) over(partition by fuid) as purchase_vip_money,
                                 sum(case
                                       when fproduct_name not rlike '(金币|银币|銀幣|游戏币|金条|金條|VIP)' then
                                        fcoins_num
                                       else
                                        0
                                     end) over(partition by fuid) as purchase_items_money,
                                 max(fcoins_num) over(partition by fuid) as max_pay_money,
                                 row_number() over(partition by fuid order by fdate desc) as ranking
                            from stage_dfqp.payment_stream_stg
                           where dt = '%(statdate)s') m4
                   where ranking = 1),
                
                --比赛报名信息
                join_match_info as
                 (select fuid as mid,
                         count(distinct flts_at) as match_enroll_count,
                         sum(fentry_fee) as match_entry_fee,
                         max(fentry_fee) as max_match_entry_fee
                    from stage_dfqp.join_gameparty_stg
                   where dt = '%(statdate)s'
                     and fitem_id = '1'
                   group by fuid),
                
                --比赛发放信息
                match_economy as
                 (select fuid as mid,
                         sum(fcost) as match_reward,
                         max(fcost) as max_match_reward
                    from stage_dfqp.match_economy_stg
                   where dt = '%(statdate)s'
                   group by fuid),
                
                --需更新的目标用户
                target_users as
                 (select mid, signup_time from veda.dfqp_user_portrait_basic)
                
                --插入更新
                insert overwrite table veda.dfqp_user_portrait_career
                select v1.mid,
                       case
                         when v2.mid is null then
                          coalesce(v10.login_days, 0)
                         else
                          coalesce(v10.login_days, 0) + 1
                       end as login_days,
                       case
                         when v3.mid is null then
                          coalesce(v10.play_days, 0)
                         else
                          coalesce(v10.play_days, 0) + 1
                       end as play_days,
                       case
                         when v5.mid is null then
                          coalesce(v10.bankrupt_days, 0)
                         else
                          coalesce(v10.bankrupt_days, 0) + 1
                       end as bankrupt_days,
                       case
                         when v6.mid is null then
                          coalesce(v10.relieve_days, 0)
                         else
                          coalesce(v10.relieve_days, 0) + 1
                       end as relieve_days,
                       case
                         when v4.mid is null then
                          coalesce(v10.match_days, 0)
                         else
                          coalesce(v10.match_days, 0) + 1
                       end as match_days,
                       coalesce(v2.login_count, 0) + coalesce(v10.login_count, 0) as login_count,
                       coalesce(v3.total_innings, 0) + coalesce(v10.total_innings, 0) as total_innings,
                       coalesce(v3.win_innings, 0) + coalesce(v10.win_innings, 0) as win_innings,
                       coalesce(v3.lose_innings, 0) + coalesce(v10.lose_innings, 0) as lose_innings,
                       coalesce(v3.play_duration, 0) + coalesce(v10.play_duration, 0) as play_duration,
                       coalesce(v7.pay_count, 0) + coalesce(v10.pay_count, 0) as pay_count,
                       coalesce(v7.pay_sum, 0) + coalesce(v10.pay_sum, 0) as pay_sum,
                       coalesce(v8.match_enroll_count, 0) + coalesce(v10.match_enroll_count, 0) as match_enroll_count,
                       coalesce(v4.match_innings, 0) + coalesce(v10.match_innings, 0) as match_innings,
                       coalesce(v4.match_duration, 0) + coalesce(v10.match_duration, 0) as match_duration,
                       coalesce(v4.match_win_innings, 0) + coalesce(v10.match_win_innings, 0) as match_win_innings,
                       coalesce(v5.bankrupt_count, 0) + coalesce(v10.bankrupt_count, 0) as bankrupt_count,
                       coalesce(v6.relieve_count, 0) + coalesce(v10.relieve_count, 0) as relieve_count,
                       coalesce(v6.relieve_silver_coins, 0) + coalesce(v10.relieve_silver_coins, 0) as relieve_silver_coins,
                       coalesce(v2.latest_login_time, v10.latest_login_time) as latest_login_time,
                       nullif(greatest(coalesce(v2.latest_login_time, '0'),
                                       coalesce(v3.latest_play_time, '0'),
                                       coalesce(v5.latest_bankrupt_time, '0'),
                                       coalesce(v6.latest_relieve_time, '0'),
                                       coalesce(v7.latest_pay_time, '0'),
                                       coalesce(v10.latest_active_time, '0')),
                              '0') as latest_active_time,
                       coalesce(v3.latest_play_time, v10.latest_play_time) as latest_play_time,
                       coalesce(v7.latest_pay_time, v10.latest_pay_time) latest_pay_time,
                       coalesce(v7.latest_pay_money, v10.latest_pay_money) as latest_pay_money,
                       coalesce(v4.latest_match_time, v10.latest_match_time) as latest_match_time,
                       coalesce(v5.latest_bankrupt_time, v10.latest_bankrupt_time) as latest_bankrupt_time,
                       coalesce(v6.latest_relieve_time, v10.latest_relieve_time) as latest_relieve_time,
                       coalesce(v10.first_login_time, v10.latest_login_time, v2.latest_login_time) as first_login_time,
                       coalesce(v10.first_play_time, v10.latest_play_time, v3.latest_play_time) as first_play_time,
                       coalesce(v10.first_pay_time, v10.latest_pay_time, v7.latest_pay_time) as first_pay_time,
                       coalesce(v10.first_match_time, v10.latest_match_time, v4.latest_match_time) as first_match_time,
                       coalesce(v10.first_bankrupt_time, v10.latest_bankrupt_time, v5.latest_bankrupt_time) as first_bankrupt_time,
                       coalesce(v10.first_relieve_time, v10.latest_relieve_time, v6.latest_relieve_time) as first_relieve_time,
                       if(v2.latest_login_time > v10.latest_login_time,
                          v10.latest_login_time,
                          v10.last_2nd_login_time) as last_2nd_login_time,
                       if(greatest(coalesce(v2.latest_login_time, '0'),
                                   coalesce(v3.latest_play_time, '0'),
                                   coalesce(v5.latest_bankrupt_time, '0'),
                                   coalesce(v6.latest_relieve_time, '0'),
                                   coalesce(v7.latest_pay_time, '0')) > coalesce(v10.latest_active_time, '0'),
                          v10.latest_active_time,
                          v10.last_2nd_active_time) as last_2nd_active_time,
                       if(v3.latest_play_time > v10.latest_play_time,
                          v10.latest_play_time,
                          v10.last_2nd_play_time) as last_2nd_play_time,
                       if(v7.latest_pay_time > v10.latest_pay_time,
                          v10.latest_pay_time,
                          v10.last_2nd_pay_time) as last_2nd_pay_time,
                       if(v4.latest_match_time > v10.latest_match_time,
                          v10.latest_match_time,
                          v10.last_2nd_match_time) as last_2nd_match_time,
                       coalesce(v2.latest_device_imei, v10.latest_device_imei) as latest_device_imei,
                       coalesce(v2.latest_device_imsi, v10.latest_device_imsi) as latest_device_imsi,
                       coalesce(v2.latest_device_type, v10.latest_device_type) as latest_device_type,
                       coalesce(v2.latest_device_pixel, v10.latest_device_pixel) as latest_device_pixel,
                       coalesce(v2.latest_device_os, v10.latest_device_os) as latest_device_os,
                       coalesce(v2.latest_app_version, v10.latest_app_version) as latest_app_version,
                       coalesce(v2.latest_network, v10.latest_network) as latest_network,
                       coalesce(v2.latest_operator, v10.latest_operator) as latest_operator,
                       coalesce(v2.latest_login_ip, v10.latest_login_ip) as latest_login_ip,
                       coalesce(v2.latest_login_ip_country, v10.latest_login_ip_country) as latest_login_ip_country,
                       coalesce(v2.latest_login_ip_province, v10.latest_login_ip_province) as latest_login_ip_province,
                       coalesce(v2.latest_login_ip_city, v10.latest_login_ip_city) as latest_login_ip_city,
                       case
                         when v2.mid is null then
                          0
                         else
                          coalesce(v10.recent_login_series_days, 0) + 1
                       end as recent_login_series_days,
                       greatest(case
                                  when v2.mid is null then
                                   0
                                  else
                                   coalesce(v10.recent_login_series_days, 0) + 1
                                end,
                                coalesce(v10.max_login_series_days, 0)) as max_login_series_days,
                       coalesce(v3.latest_pname, v10.latest_pname) as latest_pname,
                       coalesce(v3.latest_subname, v10.latest_subname) as latest_subname,
                       coalesce(v3.latest_gsubname, v10.latest_gsubname) as latest_gsubname,
                       coalesce(v3.latest_play_coins, v10.latest_play_coins) as latest_play_coins,
                       coalesce(v7.purchase_silver_money,0) + coalesce(v10.purchase_silver_money,0) as purchase_silver_money,
                       coalesce(v7.purchase_gold_money,0) + coalesce(v10.purchase_gold_money,0) as purchase_gold_money,
                       coalesce(v7.purchase_vip_money,0) + coalesce(v10.purchase_vip_money,0) as purchase_vip_money,
                       coalesce(v7.purchase_items_money,0) + coalesce(v10.purchase_items_money,0) as purchase_items_money,
                       greatest(coalesce(v7.max_pay_money,0),
                                coalesce(v10.max_pay_money,0)) as max_pay_money,
                       coalesce(v8.match_entry_fee,0) + coalesce(v10.match_entry_fee,0) as match_entry_fee,
                       greatest(coalesce(v8.max_match_entry_fee,0),
                                coalesce(v10.max_match_entry_fee,0)) as max_match_entry_fee,
                       coalesce(v4.latest_match_pname, v10.latest_match_pname) as latest_match_pname,
                       coalesce(v4.latest_match_subname, v10.latest_match_subname) as latest_match_subname,
                       coalesce(v4.latest_match_gsubname, v10.latest_match_gsubname) as latest_match_gsubname,
                       coalesce(v9.match_reward,0) + coalesce(v10.match_reward,0) as match_reward,
                       greatest(coalesce(v9.max_match_reward,0),
                                coalesce(v10.max_match_reward,0)) as max_match_reward,
                       coalesce(v3.silver_innings,0) + coalesce(v10.silver_innings,0) as silver_innings,
                       coalesce(v3.silver_duration,0) + coalesce(v10.silver_duration,0) as silver_duration,
                       coalesce(v3.silver_win_innings,0) + coalesce(v10.silver_win_innings,0) as silver_win_innings,
                       coalesce(v3.silver_lose_innings,0) + coalesce(v10.silver_lose_innings,0) as silver_lose_innings,
                       coalesce(v3.silver_service_charge,0) + coalesce(v10.silver_service_charge,0) as silver_service_charge,
                       coalesce(v3.silver_net_coins,0) + coalesce(v10.silver_net_coins,0) as silver_net_coins,
                       coalesce(v3.silver_win_coins,0) + coalesce(v10.silver_win_coins,0) as silver_win_coins,
                       coalesce(v3.silver_lose_coins,0) + coalesce(v10.silver_lose_coins,0) as silver_lose_coins,
                       greatest(coalesce(v3.silver_win_coins, 0),
                                coalesce(v10.silver_max_win_coins, 0)) as silver_max_win_coins,
                       least(coalesce(v3.silver_lose_coins, 0),
                             coalesce(v10.silver_max_lose_coins, 0)) as silver_max_lose_coins,
                       coalesce(v3.gold_innings,0) + coalesce(v10.gold_innings,0) as gold_innings,
                       coalesce(v3.gold_duration,0) + coalesce(v10.gold_duration,0) as gold_duration,
                       coalesce(v3.gold_win_innings,0) + coalesce(v10.gold_win_innings,0) as gold_win_innings,
                       coalesce(v3.gold_lose_innings,0) + coalesce(v10.gold_lose_innings,0) as gold_lose_innings,
                       coalesce(v3.gold_service_charge,0) + coalesce(v10.gold_service_charge,0) as gold_service_charge,
                       coalesce(v3.gold_net_coins,0) + coalesce(v10.gold_net_coins,0) as gold_net_coins,
                       coalesce(v3.gold_win_coins,0) + coalesce(v10.gold_win_coins,0) as gold_win_coins,
                       coalesce(v3.gold_lose_coins,0) + coalesce(v10.gold_lose_coins,0) as gold_lose_coins,
                       greatest(coalesce(v3.gold_win_coins, 0),
                                coalesce(v10.gold_max_win_coins, 0)) as gold_max_win_coins,
                       least(coalesce(v3.gold_lose_coins, 0),
                             coalesce(v10.gold_max_lose_coins, 0)) as gold_max_lose_coins,
                       coalesce(v3.score_innings,0) + coalesce(v10.score_innings,0) as score_innings,
                       coalesce(v3.score_duration,0) + coalesce(v10.score_duration,0) as score_duration,
                       coalesce(v3.score_win_innings,0) + coalesce(v10.score_win_innings,0) as score_win_innings,
                       coalesce(v3.score_lose_innings,0) + coalesce(v10.score_lose_innings,0) as score_lose_innings,
                       coalesce(v3.score_service_charge,0) + coalesce(v10.score_service_charge,0) as score_service_charge,
                       coalesce(v3.score_net_points,0) + coalesce(v10.score_net_points,0) as score_net_points,
                       coalesce(v3.score_win_points,0) + coalesce(v10.score_win_points,0) as score_win_points,
                       coalesce(v3.score_lose_points,0) + coalesce(v10.score_lose_points,0) as score_lose_points,
                       greatest(coalesce(v3.score_win_points, 0),
                                coalesce(v10.score_max_win_points, 0)) as score_max_win_points,
                       least(coalesce(v3.score_lose_points, 0),
                             coalesce(v10.score_max_lose_points, 0)) as score_max_lose_points,
                       case
                         when datediff('%(statdate)s', v1.signup_time) >= 2 then
                          'Y'
                         else
                          'N'
                       end as if_signup_2days,
                       case
                         when datediff('%(statdate)s', v1.signup_time) >= 3 then
                          'Y'
                         else
                          'N'
                       end as if_signup_3days,
                       case
                         when datediff('%(statdate)s', v1.signup_time) >= 7 then
                          'Y'
                         else
                          'N'
                       end as if_signup_7days,
                       case
                         when datediff('%(statdate)s', v1.signup_time) >= 15 then
                          'Y'
                         else
                          'N'
                       end as if_signup_15days,
                       case
                         when datediff('%(statdate)s', v1.signup_time) >= 30 then
                          'Y'
                         else
                          'N'
                       end as if_signup_30days,
                       case
                         when datediff('%(statdate)s', v1.signup_time) >= 60 then
                          'Y'
                         else
                          'N'
                       end as if_signup_60days,
                       case
                         when datediff('%(statdate)s', v1.signup_time) >= 90 then
                          'Y'
                         else
                          'N'
                       end as if_signup_90days,
                       case
                         when datediff('%(statdate)s',
                                       coalesce(v2.latest_login_time, v10.latest_login_time)) >= 7 then
                          'Y'
                         else
                          'N'
                       end as if_away_7days,
                       case
                         when datediff('%(statdate)s',
                                       coalesce(v2.latest_login_time, v10.latest_login_time)) >= 15 then
                          'Y'
                         else
                          'N'
                       end as if_away_15days,
                       case
                         when datediff('%(statdate)s',
                                       coalesce(v2.latest_login_time, v10.latest_login_time)) >= 30 then
                          'Y'
                         else
                          'N'
                       end as if_away_30days,
                       case
                         when datediff('%(statdate)s',
                                       coalesce(v2.latest_login_time, v10.latest_login_time)) >= 60 then
                          'Y'
                         else
                          'N'
                       end as if_away_60days,
                       case
                         when datediff('%(statdate)s',
                                       coalesce(v2.latest_login_time, v10.latest_login_time)) >= 90 then
                          'Y'
                         else
                          'N'
                       end as if_away_90days
                  from target_users v1
                  left join login_info v2
                    on v1.mid = v2.mid
                  left join gameparty_info_all v3
                    on v1.mid = v3.mid
                  left join match_info v4
                    on v1.mid = v4.mid
                  left join bankrupt_info v5
                    on v1.mid = v5.mid
                  left join relieve_info v6
                    on v1.mid = v6.mid
                  left join pay_info v7
                    on v1.mid = v7.mid
                  left join join_match_info v8
                    on v1.mid = v8.mid
                  left join match_economy v9
                    on v1.mid = v9.mid
                  left join veda.dfqp_user_portrait_career v10
                    on v1.mid = v10.mid
                 order by v1.mid
            """
            res = self.sql_exe(hql)
            if res != 0:
                return res
    
            return res


# 实例化执行 
a = UserPortraitCareerInfo(sys.argv[1:])
a()