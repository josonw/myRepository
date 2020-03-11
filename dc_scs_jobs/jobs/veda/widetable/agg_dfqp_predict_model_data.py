#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_dfqp_predict_model_data(BaseStatModel):
    def stat(self):
        hql = """
            --取近30天活跃用户标上价值等级
            WITH dfqp_user_score AS
             (SELECT fuid,
                     fplatformname,
                     CASE
                         WHEN play_passion >= 0
                              AND play_passion <= 9
                              AND pay_passion >= 0
                              AND pay_passion <= 9 THEN 9
                         WHEN play_passion >= 0
                              AND play_passion <= 9
                              AND pay_passion >= 10
                              AND pay_passion <= 59 THEN 7
                         WHEN play_passion >= 0
                              AND play_passion <= 9
                              AND pay_passion >= 60
                              AND pay_passion <= 100 THEN 4
                         WHEN play_passion >= 10
                              AND play_passion <= 59
                              AND pay_passion >= 0
                              AND pay_passion <= 9 THEN 8
                         WHEN play_passion >= 10
                              AND play_passion <= 59
                              AND pay_passion >= 10
                              AND pay_passion <= 59 THEN 5
                         WHEN play_passion >= 10
                              AND play_passion <= 59
                              AND pay_passion >= 60
                              AND pay_passion <= 100 THEN 2
                         WHEN play_passion >= 60
                              AND play_passion <= 100
                              AND pay_passion >= 0
                              AND pay_passion <= 9 THEN 6
                         WHEN play_passion >= 60
                              AND play_passion <= 100
                              AND pay_passion >= 10
                              AND pay_passion <= 59 THEN 3
                         WHEN play_passion >= 60
                              AND play_passion <= 100
                              AND pay_passion >= 60
                              AND pay_passion <= 100 THEN 1
                         ELSE 0
                     END AS label_score,
                     play_duration AS play_duration_30day,
                     play_innings AS play_innings_30day,
                     pay_count AS pay_count_30day,
                     pay_sum AS pay_sum_30day
              FROM veda.user_label_30day
              WHERE fgamename = '地方棋牌'
                AND dt = '%(statdate)s'),
            
            --取目标用户（近7天活跃）
              target_users as
               (select t1.fuid,
                       t1.fplatformname,
                       t2.label_score,
                       t2.play_duration_30day,
                       t2.play_innings_30day,
                       t2.pay_count_30day,
                       t2.pay_sum_30day
                  from (select distinct fuid, fplatformname
                          from stage_dfqp.user_login_stg
                         where dt >= date_sub('%(statdate)s', 6)
                           and dt <= '%(statdate)s') t1
                  left join dfqp_user_score t2
                    on (t1.fuid = t2.fuid and t1.fplatformname = t2.fplatformname)),
            
            --取各项指标数据
            a1 AS
              (SELECT mid,
                      lifespan,
                      signup_to_now_days,
                      total_silver_coin,
                      total_gold_bar,
                      carrying_silver_coin,
                      carrying_gold_bar,
                      safebox_silver_coin,
                      safebox_gold_bar,
                      CASE
                           WHEN vip_expire_time > '%(statdate)s'
                                THEN 1
                           ELSE 0
                      END AS if_vip
               FROM veda.dfqp_user_portrait_basic_history
               WHERE dt = '%(statdate)s'),
            a2 AS
              (SELECT mid,
                      play_duration,
                      total_innings,
                      win_innings,
                      lose_innings,
                      round(win_innings / total_innings, 4) AS win_rate,
                      pay_count,
                      pay_sum,
                      login_count,
                      bankrupt_count,
                      relieve_count,
                      relieve_silver_coins,
                      match_innings,
                      match_duration,
                      CASE
                           WHEN match_win_innings >= match_innings THEN 1
                           ELSE round(match_win_innings / match_innings, 4) 
                      END AS match_win_rate,
                      recent_login_series_days,
                      latest_play_coins,
                      CASE
                           WHEN '%(statdate)s' <= latest_active_time THEN 0
                           ELSE datediff('%(statdate)s', latest_active_time)
                      END AS last_active_to_now_days,
                      CASE
                           WHEN '%(statdate)s' <= latest_pay_time THEN 0
                           ELSE datediff('%(statdate)s', latest_pay_time)
                      END AS last_pay_to_now_days
               FROM veda.dfqp_user_portrait_career_history
               WHERE dt = '%(statdate)s'),
            a3 as
             (SELECT fuid,
                     fplatformname,
                     play_duration AS play_duration_7day,
                     play_innings AS play_innings_7day,
                     pay_count AS pay_count_7day,
                     pay_sum AS pay_sum_7day
              FROM veda.user_label_7day
              WHERE fgamename = '地方棋牌'
                AND dt = '%(statdate)s'),
            a4 as
             (SELECT fuid,
                     fplatformname,
                     sum(login_count) AS login_count_7day,
                     sum(bankrupt_count) AS bankrupt_count_7day,
                     sum(win_innings) AS win_innings_7day,
                     sum(lose_innings) AS lose_innings_7day,
                     sum(win_innings) / sum(win_innings + lose_innings) AS win_rate_7day
              FROM veda.user_action_everyday
              WHERE fgamename = '地方棋牌'
                AND dt >= date_sub('%(statdate)s', 6)
                AND dt <= '%(statdate)s'
              GROUP BY fuid,fplatformname)
            
            --保存结果数据到表
            insert overwrite table stage_dfqp.fiona_predict_model_data partition (dt = '%(statdate)s')
            select v.fuid,
                   v.fplatformname,
                   v.label_score,
                   coalesce(a1.lifespan, 0) as lifespan,
                   coalesce(a1.signup_to_now_days, 0) as signup_to_now_days,
                   coalesce(a1.total_silver_coin, 0) as total_silver_coin,
                   coalesce(a1.total_gold_bar, 0) as total_gold_bar,
                   coalesce(a1.carrying_silver_coin, 0) as carrying_silver_coin,
                   coalesce(a1.carrying_gold_bar, 0) as carrying_gold_bar,
                   coalesce(a1.safebox_silver_coin, 0) as safebox_silver_coin,
                   coalesce(a1.safebox_gold_bar, 0) as safebox_gold_bar,
                   coalesce(a1.if_vip, 0) as if_vip,
                   coalesce(a2.play_duration, 0) as play_duration_life,
                   coalesce(v.play_duration_30day, 0) as play_duration_30day,
                   coalesce(a3.play_duration_7day, 0) as play_duration_7day,
                   coalesce(a2.total_innings, 0) as play_innings_life,
                   coalesce(v.play_innings_30day, 0) as play_innings_30day,
                   coalesce(a3.play_innings_7day, 0) as play_innings_7day,
                   coalesce(a2.win_innings, 0) as win_innings_life,
                   coalesce(a4.win_innings_7day, 0) as win_innings_7day,
                   coalesce(a2.lose_innings, 0) as lose_innings_life,
                   coalesce(a4.lose_innings_7day, 0) as lose_innings_7day,
                   coalesce(a2.win_rate, 0) as win_rate_life,
                   coalesce(round(a4.win_rate_7day,4), 0) as win_rate_7day,
                   coalesce(a2.pay_count, 0) as pay_count_life,
                   coalesce(v.pay_count_30day, 0) as pay_count_30day,
                   coalesce(a3.pay_count_7day, 0) as pay_count_7day,
                   coalesce(a2.pay_sum, 0) as pay_sum_life,
                   coalesce(v.pay_sum_30day, 0) as pay_sum_30day,
                   coalesce(a3.pay_sum_7day, 0) as pay_sum_7day,
                   coalesce(a2.login_count, 0) as login_count_life,
                   coalesce(a4.login_count_7day, 0) as login_count_7day,
                   coalesce(a2.bankrupt_count, 0) as bankrupt_count_life,
                   coalesce(a4.bankrupt_count_7day, 0) as bankrupt_count_7day,
                   coalesce(a2.relieve_count, 0) as relieve_count_life,
                   coalesce(a2.relieve_silver_coins, 0) as relieve_silver_coins_life,
                   coalesce(a2.match_innings, 0) as match_innings_life,
                   coalesce(a2.match_duration, 0) as match_duration_life,
                   coalesce(a2.match_win_rate, 0) as match_win_rate_life,
                   coalesce(a2.recent_login_series_days, 0) as recent_login_series_days,
                   coalesce(a2.latest_play_coins, 0) as latest_play_coins,
                   coalesce(a2.last_active_to_now_days, 0) as last_active_to_now_days,
                   coalesce(a2.last_pay_to_now_days, 0) as last_pay_to_now_days,
                   null as if_away
              from target_users v
              left join a1
                on v.fuid = a1.mid
              left join a2
                on v.fuid = a2.mid
              left join a3
                on v.fuid = a3.fuid
               and v.fplatformname = a3.fplatformname
              left join a4
                on v.fuid = a4.fuid
               and v.fplatformname = a4.fplatformname
             where v.label_score > 0
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            --给7天前的数据标记留存流失
            with last_week_data as
             (select *
                from stage_dfqp.fiona_predict_model_data
               where dt = date_sub('%(statdate)s', 7))
            insert overwrite table stage_dfqp.fiona_predict_model_data partition (dt)
            select t1.fuid,
                   t1.fplatformname,
                   t1.label_score,
                   t1.lifespan,
                   t1.signup_to_now_days,
                   t1.total_silver_coin,
                   t1.total_gold_bar,
                   t1.carrying_silver_coin,
                   t1.carrying_gold_bar,
                   t1.safebox_silver_coin,
                   t1.safebox_gold_bar,
                   t1.if_vip,
                   t1.play_duration_life,
                   t1.play_duration_30day,
                   t1.play_duration_7day,
                   t1.play_innings_life,
                   t1.play_innings_30day,
                   t1.play_innings_7day,
                   t1.win_innings_life,
                   t1.win_innings_7day,
                   t1.lose_innings_life,
                   t1.lose_innings_7day,
                   t1.win_rate_life,
                   t1.win_rate_7day,
                   t1.pay_count_life,
                   t1.pay_count_30day,
                   t1.pay_count_7day,
                   t1.pay_sum_life,
                   t1.pay_sum_30day,
                   t1.pay_sum_7day,
                   t1.login_count_life,
                   t1.login_count_7day,
                   t1.bankrupt_count_life,
                   t1.bankrupt_count_7day,
                   t1.relieve_count_life,
                   t1.relieve_silver_coins_life,
                   t1.match_innings_life,
                   t1.match_duration_life,
                   t1.match_win_rate_life,
                   t1.recent_login_series_days,
                   t1.latest_play_coins,
                   t1.last_active_to_now_days,
                   t1.last_pay_to_now_days,
                   case
                     when t2.fuid is null then
                      1
                     else
                      0
                   end as if_away,
                   date_sub('%(statdate)s', 7) as dt
              from last_week_data t1
              left join (select distinct fuid, fplatformname
                           from stage_dfqp.user_login_stg
                          where dt >= date_sub('%(statdate)s', 6)
                            and dt <= '%(statdate)s') t2
                on (t1.fuid = t2.fuid and t1.fplatformname = t2.fplatformname)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
a = agg_dfqp_predict_model_data(sys.argv[1:])
a()
