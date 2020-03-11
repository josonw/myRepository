#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserPortraitLabel(BaseStatModel):
    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS work.user_action_temp_%(statdatenum)s
            (
              fgamefsk BIGINT COMMENT '游戏ID',
              fgamename STRING COMMENT '游戏名称',
              fplatformfsk BIGINT COMMENT '平台ID',
              fplatformname STRING COMMENT '平台名称',
              fuid BIGINT COMMENT '用户ID',
              if_new_user CHAR(1) COMMENT '是否新用户',
              play_duration BIGINT COMMENT '玩牌时长(秒)',
              play_innings BIGINT COMMENT '玩牌局数',
              pay_sum DECIMAL(20,2) COMMENT '付费额度(原币)',
              pay_sum_usd DECIMAL(10,2) COMMENT '付费额度(美元)',
              pay_count BIGINT COMMENT '付费次数',
              match_duration BIGINT COMMENT '比赛时长(秒)',
              match_innings BIGINT COMMENT '比赛局数'
            )
            COMMENT '用户当天行为数据'
            STORED AS PARQUET
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        
        hql = """
            CREATE TABLE IF NOT EXISTS veda.user_label_day
            (
              fgamefsk BIGINT COMMENT '游戏ID',
              fgamename STRING COMMENT '游戏名称',
              fplatformfsk BIGINT COMMENT '平台ID',
              fplatformname STRING COMMENT '平台名称',
              fuid bigint COMMENT '用户ID',
              if_new_user CHAR(1) COMMENT '是否新用户',
              play_passion BIGINT COMMENT '玩牌热情',
              play_duration BIGINT COMMENT '当天玩牌时长（秒）',
              play_innings BIGINT COMMENT '当天玩牌局数',
              pay_passion BIGINT COMMENT '付费热情',
              pay_sum DECIMAL(20,2) COMMENT '当天付费额度(原币)',
              pay_sum_usd DECIMAL(10,2) COMMENT '当天付费额度(美元)',
              pay_count BIGINT COMMENT '当天付费次数',
              match_passion BIGINT COMMENT '比赛热情',
              match_duration BIGINT COMMENT '当天比赛时长（秒）',
              match_innings BIGINT COMMENT '当天比赛局数'
            )
            COMMENT '用户标签（每日）'
            PARTITIONED BY (dt STRING)
            STORED AS PARQUET
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            CREATE TABLE IF NOT EXISTS veda.user_label_7day
            (
              fgamefsk BIGINT COMMENT '游戏ID',
              fgamename STRING COMMENT '游戏名称',
              fplatformfsk BIGINT COMMENT '平台ID',
              fplatformname STRING COMMENT '平台名称',
              fuid bigint COMMENT '用户ID',
              play_passion BIGINT COMMENT '玩牌热情',
              play_days BIGINT COMMENT '周玩牌天数',
              play_duration BIGINT COMMENT '周玩牌时长（秒）',
              play_innings BIGINT COMMENT '周玩牌局数',
              pay_passion BIGINT COMMENT '付费热情',
              pay_sum DECIMAL(20,2) COMMENT '周付费额度(原币)',
              pay_sum_usd DECIMAL(10,2) COMMENT '周付费额度(美元)',
              pay_count BIGINT COMMENT '周付费次数',
              match_passion BIGINT COMMENT '比赛热情',
              match_duration BIGINT COMMENT '周比赛时长（秒）',
              match_innings BIGINT COMMENT '周比赛局数'
            )
            COMMENT '用户标签（近7日）'
            PARTITIONED BY (dt STRING)
            STORED AS PARQUET
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            CREATE TABLE IF NOT EXISTS veda.user_label_30day
            (
              fgamefsk BIGINT COMMENT '游戏ID',
              fgamename STRING COMMENT '游戏名称',
              fplatformfsk BIGINT COMMENT '平台ID',
              fplatformname STRING COMMENT '平台名称',
              fuid bigint COMMENT '用户ID',
              play_passion BIGINT COMMENT '玩牌热情',
              play_days BIGINT COMMENT '月玩牌天数',
              play_duration BIGINT COMMENT '月玩牌时长（秒）',
              play_innings BIGINT COMMENT '月玩牌局数',
              pay_passion BIGINT COMMENT '付费热情',
              pay_sum DECIMAL(20,2) COMMENT '月付费额度(原币)',
              pay_sum_usd DECIMAL(10,2) COMMENT '月付费额度(美元)',
              pay_count BIGINT COMMENT '月付费次数',
              match_passion BIGINT COMMENT '比赛热情',
              match_duration BIGINT COMMENT '月比赛时长（秒）',
              match_innings BIGINT COMMENT '月比赛局数'
            )
            COMMENT '用户标签（近30日）'
            PARTITIONED BY (dt STRING)
            STORED AS PARQUET
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        hql = """
            with signup_info as
             (select distinct fgamefsk, fgamename, fplatformfsk, fplatformname, fuid
                from dim.bpid_map t1
               inner join stage.user_signup_stg t2
                  on t1.fbpid = t2.fbpid
               where dt = '%(statdate)s'),
            gameparty_info as
             (select fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fuid,
                     sum(case
                           when unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                10800 then
                            unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                           else
                            0
                         end) as play_duration,
                     count(distinct finning_id) as play_innings,
                     sum(case
                           when fmatch_id != '0' and
                                unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                10800 then
                            unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                           else
                            0
                         end) as match_duration,
                     count(distinct(case
                                      when fmatch_id != '0' then
                                       finning_id
                                      else
                                       null
                                    end)) as match_innings
                from dim.bpid_map t1
               inner join stage.user_gameparty_stg t2
                  on t1.fbpid = t2.fbpid
               where dt = '%(statdate)s'
               group by fgamefsk, fgamename, fplatformfsk, fplatformname, fuid),
            payment_info as
             (select fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fuid,
                     sum(fcoins_num) as pay_sum,
                     sum(fpamount_usd) as pay_sum_usd,
                     count(distinct fdate) as pay_count
                from dim.bpid_map t1
               inner join stage.payment_stream_stg t2
                  on t1.fbpid = t2.fbpid
               where dt = '%(statdate)s'
               group by fgamefsk, fgamename, fplatformfsk, fplatformname, fuid),
            target_users as
             (select distinct fgamefsk, fgamename, fplatformfsk, fplatformname, fuid
                from signup_info
               union distinct
              select distinct fgamefsk, fgamename, fplatformfsk, fplatformname, fuid
                from gameparty_info
               union distinct
              select distinct fgamefsk, fgamename, fplatformfsk, fplatformname, fuid
                from payment_info)
            insert overwrite table work.user_action_temp_%(statdatenum)s
            select v1.fgamefsk,
                   v1.fgamename,
                   v1.fplatformfsk,
                   v1.fplatformname,
                   v1.fuid,
                   case when v2.fuid is not null then 'Y' else 'N' end as if_new_user,
                   coalesce(v3.play_duration, 0) as play_duration,
                   coalesce(v3.play_innings, 0) as play_innings,
                   coalesce(v4.pay_sum, 0) as pay_sum,
                   coalesce(v4.pay_sum_usd, 0) as pay_sum_usd,
                   coalesce(v4.pay_count, 0) as pay_count,
                   coalesce(v3.match_duration, 0) as match_duration,
                   coalesce(v3.match_innings, 0) as match_innings
              from target_users v1
              left join signup_info v2
                on (v1.fgamefsk = v2.fgamefsk and v1.fplatformfsk = v2.fplatformfsk and v1.fuid = v2.fuid)
              left join gameparty_info v3
                on (v1.fgamefsk = v3.fgamefsk and v1.fplatformfsk = v3.fplatformfsk and v1.fuid = v3.fuid)
              left join payment_info v4
                on (v1.fgamefsk = v4.fgamefsk and v1.fplatformfsk = v4.fplatformfsk and v1.fuid = v4.fuid)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        
        hql = """
            with play_duration_info as
             (select fgamefsk,
                     fplatformfsk,
                     play_duration,
                     sum(play_duration_subarea) over(partition by fgamefsk, fplatformfsk order by play_duration rows between unbounded preceding and current row) as play_duration_position,
                     play_duration_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      play_duration,
                                      sum(play_duration) over(partition by fgamefsk, fplatformfsk, play_duration) as play_duration_subarea,
                                      sum(play_duration) over(partition by fgamefsk, fplatformfsk) as play_duration_area
                        from work.user_action_temp_%(statdatenum)s) m1),
            play_innings_info as
             (select fgamefsk,
                     fplatformfsk,
                     play_innings,
                     sum(play_innings_subarea) over(partition by fgamefsk, fplatformfsk order by play_innings rows between unbounded preceding and current row) as play_innings_position,
                     play_innings_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      play_innings,
                                      sum(play_innings) over(partition by fgamefsk, fplatformfsk, play_innings) as play_innings_subarea,
                                      sum(play_innings) over(partition by fgamefsk, fplatformfsk) as play_innings_area
                        from work.user_action_temp_%(statdatenum)s) m2),
            pay_sum_info as
             (select fgamefsk,
                     fplatformfsk,
                     pay_sum_usd,
                     sum(pay_sum_subarea) over(partition by fgamefsk, fplatformfsk order by pay_sum_usd rows between unbounded preceding and current row) as pay_sum_position,
                     pay_sum_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      pay_sum_usd,
                                      sum(pay_sum_usd) over(partition by fgamefsk, fplatformfsk, pay_sum_usd) as pay_sum_subarea,
                                      sum(pay_sum_usd) over(partition by fgamefsk, fplatformfsk) as pay_sum_area
                        from work.user_action_temp_%(statdatenum)s) m3),
            pay_count_info as
             (select fgamefsk,
                     fplatformfsk,
                     pay_count,
                     sum(pay_count_subarea) over(partition by fgamefsk, fplatformfsk order by pay_count rows between unbounded preceding and current row) as pay_count_position,
                     pay_count_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      pay_count,
                                      sum(pay_count) over(partition by fgamefsk, fplatformfsk, pay_count) as pay_count_subarea,
                                      sum(pay_count) over(partition by fgamefsk, fplatformfsk) as pay_count_area
                        from work.user_action_temp_%(statdatenum)s) m4),
            match_duration_info as
             (select fgamefsk,
                     fplatformfsk,
                     match_duration,
                     sum(match_duration_subarea) over(partition by fgamefsk, fplatformfsk order by match_duration rows between unbounded preceding and current row) as match_duration_position,
                     match_duration_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      match_duration,
                                      sum(match_duration) over(partition by fgamefsk, fplatformfsk, match_duration) as match_duration_subarea,
                                      sum(match_duration) over(partition by fgamefsk, fplatformfsk) as match_duration_area
                        from work.user_action_temp_%(statdatenum)s) m5),
            match_innings_info as
             (select fgamefsk,
                     fplatformfsk,
                     match_innings,
                     sum(match_innings_subarea) over(partition by fgamefsk, fplatformfsk order by match_innings rows between unbounded preceding and current row) as match_innings_position,
                     match_innings_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      match_innings,
                                      sum(match_innings) over(partition by fgamefsk, fplatformfsk, match_innings) as match_innings_subarea,
                                      sum(match_innings) over(partition by fgamefsk, fplatformfsk) as match_innings_area
                        from work.user_action_temp_%(statdatenum)s) m6)
            insert overwrite table veda.user_label_day partition(dt = '%(statdate)s')
            select t1.fgamefsk,
                   t1.fgamename,
                   t1.fplatformfsk,
                   t1.fplatformname,
                   t1.fuid,
                   t1.if_new_user,
                   coalesce(ceil(t2.play_duration_position * 50 / t2.play_duration_area +
                                 t3.play_innings_position * 50 / t3.play_innings_area),
                            0) as play_passion,
                   t2.play_duration,
                   t3.play_innings,
                   coalesce(ceil(t4.pay_sum_position * 70 / t4.pay_sum_area +
                                 t5.pay_count_position * 30 / t5.pay_count_area),
                            0) as pay_passion,
                   t1.pay_sum,
                   t4.pay_sum_usd,
                   t5.pay_count,
                   coalesce(ceil(t6.match_duration_position * 70 / t6.match_duration_area +
                                 t7.match_innings_position * 30 / t7.match_innings_area),
                            0) as match_passion,
                   t6.match_duration,
                   t7.match_innings
              from work.user_action_temp_%(statdatenum)s t1
             inner join play_duration_info t2
                on (t1.fgamefsk = t2.fgamefsk and t1.fplatformfsk = t2.fplatformfsk and
                   t1.play_duration = t2.play_duration)
             inner join play_innings_info t3
                on (t1.fgamefsk = t3.fgamefsk and t1.fplatformfsk = t3.fplatformfsk and
                   t1.play_innings = t3.play_innings)
             inner join pay_sum_info t4
                on (t1.fgamefsk = t4.fgamefsk and t1.fplatformfsk = t4.fplatformfsk and
                   t1.pay_sum_usd = t4.pay_sum_usd)
             inner join pay_count_info t5
                on (t1.fgamefsk = t5.fgamefsk and t1.fplatformfsk = t5.fplatformfsk and
                   t1.pay_count = t5.pay_count)
             inner join match_duration_info t6
                on (t1.fgamefsk = t6.fgamefsk and t1.fplatformfsk = t6.fplatformfsk and
                   t1.match_duration = t6.match_duration)
             inner join match_innings_info t7
                on (t1.fgamefsk = t7.fgamefsk and t1.fplatformfsk = t7.fplatformfsk and
                   t1.match_innings = t7.match_innings)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            with user_action_week as
             (select fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fuid,
                     count(distinct dt) as play_days,
                     sum(play_duration) as play_duration,
                     sum(play_innings) as play_innings,
                     sum(pay_sum) as pay_sum,
                     sum(pay_sum_usd) as pay_sum_usd,
                     sum(pay_count) as pay_count,
                     sum(match_duration) as match_duration,
                     sum(match_innings) as match_innings
                from veda.user_label_day
               where dt >= date_sub('%(statdate)s', 6)
                 and dt <= '%(statdate)s'
               group by fgamefsk, fgamename, fplatformfsk, fplatformname, fuid),
            play_duration_info as
             (select fgamefsk,
                     fplatformfsk,
                     play_duration,
                     sum(play_duration_subarea) over(partition by fgamefsk, fplatformfsk order by play_duration rows between unbounded preceding and current row) as play_duration_position,
                     play_duration_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      play_duration,
                                      sum(play_duration) over(partition by fgamefsk, fplatformfsk, play_duration) as play_duration_subarea,
                                      sum(play_duration) over(partition by fgamefsk, fplatformfsk) as play_duration_area
                        from user_action_week) m1),
            play_days_info as
             (select fgamefsk,
                     fplatformfsk,
                     play_days,
                     sum(play_days_subarea) over(partition by fgamefsk, fplatformfsk order by play_days rows between unbounded preceding and current row) as play_days_position,
                     play_days_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      play_days,
                                      sum(play_days) over(partition by fgamefsk, fplatformfsk, play_days) as play_days_subarea,
                                      sum(play_days) over(partition by fgamefsk, fplatformfsk) as play_days_area
                        from user_action_week) m2),
            pay_sum_info as
             (select fgamefsk,
                     fplatformfsk,
                     pay_sum_usd,
                     sum(pay_sum_subarea) over(partition by fgamefsk, fplatformfsk order by pay_sum_usd rows between unbounded preceding and current row) as pay_sum_position,
                     pay_sum_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      pay_sum_usd,
                                      sum(pay_sum_usd) over(partition by fgamefsk, fplatformfsk, pay_sum_usd) as pay_sum_subarea,
                                      sum(pay_sum_usd) over(partition by fgamefsk, fplatformfsk) as pay_sum_area
                        from user_action_week) m3),
            pay_count_info as
             (select fgamefsk,
                     fplatformfsk,
                     pay_count,
                     sum(pay_count_subarea) over(partition by fgamefsk, fplatformfsk order by pay_count rows between unbounded preceding and current row) as pay_count_position,
                     pay_count_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      pay_count,
                                      sum(pay_count) over(partition by fgamefsk, fplatformfsk, pay_count) as pay_count_subarea,
                                      sum(pay_count) over(partition by fgamefsk, fplatformfsk) as pay_count_area
                        from user_action_week) m4),
            match_duration_info as
             (select fgamefsk,
                     fplatformfsk,
                     match_duration,
                     sum(match_duration_subarea) over(partition by fgamefsk, fplatformfsk order by match_duration rows between unbounded preceding and current row) as match_duration_position,
                     match_duration_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      match_duration,
                                      sum(match_duration) over(partition by fgamefsk, fplatformfsk, match_duration) as match_duration_subarea,
                                      sum(match_duration) over(partition by fgamefsk, fplatformfsk) as match_duration_area
                        from user_action_week) m5),
            match_innings_info as
             (select fgamefsk,
                     fplatformfsk,
                     match_innings,
                     sum(match_innings_subarea) over(partition by fgamefsk, fplatformfsk order by match_innings rows between unbounded preceding and current row) as match_innings_position,
                     match_innings_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      match_innings,
                                      sum(match_innings) over(partition by fgamefsk, fplatformfsk, match_innings) as match_innings_subarea,
                                      sum(match_innings) over(partition by fgamefsk, fplatformfsk) as match_innings_area
                        from user_action_week) m6)
            insert overwrite table veda.user_label_7day partition(dt = '%(statdate)s')
            select t1.fgamefsk,
                   t1.fgamename,
                   t1.fplatformfsk,
                   t1.fplatformname,
                   t1.fuid,
                   coalesce(ceil(t2.play_duration_position * 50 / t2.play_duration_area +
                                 t3.play_days_position * 50 / t3.play_days_area),
                            0) as play_passion,
                   t1.play_days,
                   t1.play_duration,
                   t1.play_innings,
                   coalesce(ceil(t4.pay_sum_position * 70 / t4.pay_sum_area +
                                 t5.pay_count_position * 30 / t5.pay_count_area),
                            0) as pay_passion,
                   t1.pay_sum,
                   t1.pay_sum_usd,
                   t1.pay_count,
                   coalesce(ceil(t6.match_duration_position * 70 /
                                 t6.match_duration_area +
                                 t7.match_innings_position * 30 / t7.match_innings_area),
                            0) as match_passion,
                   t1.match_duration,
                   t1.match_innings
              from user_action_week t1
             inner join play_duration_info t2
                on (t1.fgamefsk = t2.fgamefsk and t1.fplatformfsk = t2.fplatformfsk and
                   t1.play_duration = t2.play_duration)
             inner join play_days_info t3
                on (t1.fgamefsk = t3.fgamefsk and t1.fplatformfsk = t3.fplatformfsk and
                   t1.play_days = t3.play_days)
             inner join pay_sum_info t4
                on (t1.fgamefsk = t4.fgamefsk and t1.fplatformfsk = t4.fplatformfsk and
                   t1.pay_sum_usd = t4.pay_sum_usd)
             inner join pay_count_info t5
                on (t1.fgamefsk = t5.fgamefsk and t1.fplatformfsk = t5.fplatformfsk and
                   t1.pay_count = t5.pay_count)
             inner join match_duration_info t6
                on (t1.fgamefsk = t6.fgamefsk and t1.fplatformfsk = t6.fplatformfsk and
                   t1.match_duration = t6.match_duration)
             inner join match_innings_info t7
                on (t1.fgamefsk = t7.fgamefsk and t1.fplatformfsk = t7.fplatformfsk and
                   t1.match_innings = t7.match_innings)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            with user_action_month as
             (select fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fuid,
                     count(distinct dt) as play_days,
                     sum(play_duration) as play_duration,
                     sum(play_innings) as play_innings,
                     sum(pay_sum) as pay_sum,
                     sum(pay_sum_usd) as pay_sum_usd,
                     sum(pay_count) as pay_count,
                     sum(match_duration) as match_duration,
                     sum(match_innings) as match_innings
                from veda.user_label_day
               where dt >= date_sub('%(statdate)s', 29)
                 and dt <= '%(statdate)s'
               group by fgamefsk, fgamename, fplatformfsk, fplatformname, fuid),
            play_duration_info as
             (select fgamefsk,
                     fplatformfsk,
                     play_duration,
                     sum(play_duration_subarea) over(partition by fgamefsk, fplatformfsk order by play_duration rows between unbounded preceding and current row) as play_duration_position,
                     play_duration_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      play_duration,
                                      sum(play_duration) over(partition by fgamefsk, fplatformfsk, play_duration) as play_duration_subarea,
                                      sum(play_duration) over(partition by fgamefsk, fplatformfsk) as play_duration_area
                        from user_action_month) m1),
            play_days_info as
             (select fgamefsk,
                     fplatformfsk,
                     play_days,
                     sum(play_days_subarea) over(partition by fgamefsk, fplatformfsk order by play_days rows between unbounded preceding and current row) as play_days_position,
                     play_days_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      play_days,
                                      sum(play_days) over(partition by fgamefsk, fplatformfsk, play_days) as play_days_subarea,
                                      sum(play_days) over(partition by fgamefsk, fplatformfsk) as play_days_area
                        from user_action_month) m2),
            pay_sum_info as
             (select fgamefsk,
                     fplatformfsk,
                     pay_sum_usd,
                     sum(pay_sum_subarea) over(partition by fgamefsk, fplatformfsk order by pay_sum_usd rows between unbounded preceding and current row) as pay_sum_position,
                     pay_sum_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      pay_sum_usd,
                                      sum(pay_sum_usd) over(partition by fgamefsk, fplatformfsk, pay_sum_usd) as pay_sum_subarea,
                                      sum(pay_sum_usd) over(partition by fgamefsk, fplatformfsk) as pay_sum_area
                        from user_action_month) m3),
            pay_count_info as
             (select fgamefsk,
                     fplatformfsk,
                     pay_count,
                     sum(pay_count_subarea) over(partition by fgamefsk, fplatformfsk order by pay_count rows between unbounded preceding and current row) as pay_count_position,
                     pay_count_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      pay_count,
                                      sum(pay_count) over(partition by fgamefsk, fplatformfsk, pay_count) as pay_count_subarea,
                                      sum(pay_count) over(partition by fgamefsk, fplatformfsk) as pay_count_area
                        from user_action_month) m4),
            match_duration_info as
             (select fgamefsk,
                     fplatformfsk,
                     match_duration,
                     sum(match_duration_subarea) over(partition by fgamefsk, fplatformfsk order by match_duration rows between unbounded preceding and current row) as match_duration_position,
                     match_duration_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      match_duration,
                                      sum(match_duration) over(partition by fgamefsk, fplatformfsk, match_duration) as match_duration_subarea,
                                      sum(match_duration) over(partition by fgamefsk, fplatformfsk) as match_duration_area
                        from user_action_month) m5),
            match_innings_info as
             (select fgamefsk,
                     fplatformfsk,
                     match_innings,
                     sum(match_innings_subarea) over(partition by fgamefsk, fplatformfsk order by match_innings rows between unbounded preceding and current row) as match_innings_position,
                     match_innings_area
                from (select distinct fgamefsk,
                                      fplatformfsk,
                                      match_innings,
                                      sum(match_innings) over(partition by fgamefsk, fplatformfsk, match_innings) as match_innings_subarea,
                                      sum(match_innings) over(partition by fgamefsk, fplatformfsk) as match_innings_area
                        from user_action_month) m6)
            insert overwrite table veda.user_label_30day partition(dt = '%(statdate)s')
            select t1.fgamefsk,
                   t1.fgamename,
                   t1.fplatformfsk,
                   t1.fplatformname,
                   t1.fuid,
                   coalesce(ceil(t2.play_duration_position * 50 / t2.play_duration_area +
                                 t3.play_days_position * 50 / t3.play_days_area),
                            0) as play_passion,
                   t1.play_days,
                   t1.play_duration,
                   t1.play_innings,
                   coalesce(ceil(t4.pay_sum_position * 70 / t4.pay_sum_area +
                                 t5.pay_count_position * 30 / t5.pay_count_area),
                            0) as pay_passion,
                   t1.pay_sum,
                   t1.pay_sum_usd,
                   t1.pay_count,
                   coalesce(ceil(t6.match_duration_position * 70 /
                                 t6.match_duration_area +
                                 t7.match_innings_position * 30 / t7.match_innings_area),
                            0) as match_passion,
                   t1.match_duration,
                   t1.match_innings
              from user_action_month t1
             inner join play_duration_info t2
                on (t1.fgamefsk = t2.fgamefsk and t1.fplatformfsk = t2.fplatformfsk and
                   t1.play_duration = t2.play_duration)
             inner join play_days_info t3
                on (t1.fgamefsk = t3.fgamefsk and t1.fplatformfsk = t3.fplatformfsk and
                   t1.play_days = t3.play_days)
             inner join pay_sum_info t4
                on (t1.fgamefsk = t4.fgamefsk and t1.fplatformfsk = t4.fplatformfsk and
                   t1.pay_sum_usd = t4.pay_sum_usd)
             inner join pay_count_info t5
                on (t1.fgamefsk = t5.fgamefsk and t1.fplatformfsk = t5.fplatformfsk and
                   t1.pay_count = t5.pay_count)
             inner join match_duration_info t6
                on (t1.fgamefsk = t6.fgamefsk and t1.fplatformfsk = t6.fplatformfsk and
                   t1.match_duration = t6.match_duration)
             inner join match_innings_info t7
                on (t1.fgamefsk = t7.fgamefsk and t1.fplatformfsk = t7.fplatformfsk and
                   t1.match_innings = t7.match_innings)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
a = UserPortraitLabel(sys.argv[1:])
a()