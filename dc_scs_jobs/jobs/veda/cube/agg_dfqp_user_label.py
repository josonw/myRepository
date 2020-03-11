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
            create table if not exists veda.dfqp_subgame_user_label_day
            (
              fplatformfsk BIGINT COMMENT '平台ID',
              fplatformname STRING COMMENT '平台名称',
              fgame_id INT COMMENT '子游戏ID',
              fpname STRING COMMENT '子游戏名称',
              fuid bigint comment '用户ID',
              if_new_user CHAR(1) COMMENT '是否新用户',
              play_passion int comment '玩牌热情',
              play_label string comment '玩牌标签',
              play_duration int comment '当天玩牌时长（秒）',
              play_innings int comment '当天玩牌局数',
              match_passion int comment '比赛热情',
              match_label string comment '比赛标签',
              match_duration int comment '当天比赛时长（秒）',
              match_innings int comment '当天比赛局数'
            )
            comment '地方棋牌子游戏用户标签（每日）'
            partitioned by (dt string)
            stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            create table if not exists veda.dfqp_subgame_user_label_7day
            (
              fplatformfsk BIGINT COMMENT '平台ID',
              fplatformname STRING COMMENT '平台名称',
              fgame_id INT COMMENT '子游戏ID',
              fpname STRING COMMENT '子游戏名称',
              fuid bigint comment '用户ID',
              play_passion int comment '玩牌热情',
              play_label string comment '玩牌标签',
              play_days INT COMMENT '周玩牌天数',
              play_duration int comment '周玩牌时长（秒）',
              play_innings int comment '周玩牌局数',
              match_passion int comment '比赛热情',
              match_label string comment '比赛标签',
              match_duration int comment '周比赛时长（秒）',
              match_innings int comment '周比赛局数'
            )
            comment '地方棋牌子游戏用户标签（近7日）'
            partitioned by (dt string)
            stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            create table if not exists veda.dfqp_subgame_user_label_30day
            (
              fplatformfsk BIGINT COMMENT '平台ID',
              fplatformname STRING COMMENT '平台名称',
              fgame_id INT COMMENT '子游戏ID',
              fpname STRING COMMENT '子游戏名称',
              fuid bigint comment '用户ID',
              play_passion int comment '玩牌热情',
              play_label string comment '玩牌标签',
              play_days INT COMMENT '月玩牌天数',
              play_duration int comment '月玩牌时长（秒）',
              play_innings int comment '月玩牌局数',
              match_passion int comment '比赛热情',
              match_label string comment '比赛标签',
              match_duration int comment '月比赛时长（秒）',
              match_innings int comment '月比赛局数'
            )
            comment '地方棋牌子游戏用户标签（近30日）'
            partitioned by (dt string)
            stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        hql = """
            with signup_info as
             (select distinct fplatformfsk, fplatformname, fuid
                from stage_dfqp.user_signup_stg
               where dt = '%(statdate)s'),
            gameparty_info as
             (select fplatformfsk,
                     fplatformname,
                     fgame_id,
                     fpname,
                     fuid,
                     sum(case
                           when unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                10800 then
                            unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                           else
                            0
                         end) as game_duration,
                     sum(case
                           when (fmatch_id = '0' or fmatch_id is null) and
                                unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                10800 then
                            unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                           else
                            0
                         end) as basic_duration,
                     sum(case
                           when fmatch_id != '0' and
                                unix_timestamp(fe_timer) - unix_timestamp(fs_timer) between 1 and
                                10800 then
                            unix_timestamp(fe_timer) - unix_timestamp(fs_timer)
                           else
                            0
                         end) as match_duration,
                     count(distinct finning_id) as game_innings,
                     count(distinct(case
                                      when fmatch_id = '0' or fmatch_id is null then
                                       finning_id
                                      else
                                       null
                                    end)) as basic_innings,
                     count(distinct(case
                                      when fmatch_id != '0' then
                                       finning_id
                                      else
                                       null
                                    end)) as match_innings
                from stage_dfqp.user_gameparty_stg
               where dt = '%(statdate)s'
               group by fplatformfsk,
                        fplatformname,
                        fgame_id,
                        fpname,
                        fuid),
            user_action_data as
             (select v1.fplatformfsk,
                     v1.fplatformname,
                     v1.fgame_id,
                     v1.fpname,
                     v1.fuid,
                     case
                       when v2.fuid is not null then
                        'Y'
                       else
                        'N'
                     end as if_new_user,
                     coalesce(game_duration, 0) as game_duration,
                     coalesce(basic_duration, 0) as basic_duration,
                     coalesce(match_duration, 0) as match_duration,
                     coalesce(game_innings, 0) as game_innings,
                     coalesce(basic_innings, 0) as basic_innings,
                     coalesce(match_innings, 0) as match_innings
                from gameparty_info v1
                left join signup_info v2
                  on (v1.fplatformfsk = v2.fplatformfsk and v1.fuid = v2.fuid)),
            label_data_day as
             (select fplatformfsk,
                     fplatformname,
                     fgame_id,
                     fpname,
                     fuid,
                     if_new_user,
                     basic_duration as play_duration,
                     basic_innings as play_innings,
                     match_duration,
                     match_innings,
                     sum(basic_duration) over(partition by fplatformfsk, fgame_id order by basic_duration desc nulls last rows between unbounded preceding and current row) as play_duration_position,
                     sum(basic_innings) over(partition by fplatformfsk, fgame_id order by basic_innings desc nulls last rows between unbounded preceding and current row) as play_innings_position,
                     sum(match_duration) over(partition by fplatformfsk, fgame_id order by match_duration desc nulls last rows between unbounded preceding and current row) as match_duration_position,
                     sum(match_innings) over(partition by fplatformfsk, fgame_id order by match_innings desc nulls last rows between unbounded preceding and current row) as match_innings_position,
                     sum(basic_duration) over(partition by fplatformfsk, fgame_id) as play_duration_area,
                     sum(basic_innings) over(partition by fplatformfsk, fgame_id) as play_innings_area,
                     sum(match_duration) over(partition by fplatformfsk, fgame_id) as match_duration_area,
                     sum(match_innings) over(partition by fplatformfsk, fgame_id) as match_innings_area
                from user_action_data)
            insert overwrite table veda.dfqp_subgame_user_label_day partition (dt = '%(statdate)s')
            select fplatformfsk,
                   fplatformname,
                   fgame_id,
                   fpname,
                   fuid,
                   if_new_user,
                   coalesce(100 -
                            floor(play_duration_position * 50 / play_duration_area +
                                  play_innings_position * 50 / play_innings_area),
                            0) as play_passion,
                   cast(null as string) as play_label,
                   play_duration,
                   play_innings,
                   coalesce(100 -
                            floor(match_duration_position * 50 / match_duration_area +
                                  match_innings_position * 50 / match_innings_area),
                            0) as match_passion,
                   cast(null as string) as match_label,
                   match_duration,
                   match_innings
              from label_data_day
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            with label_data_7day as
             (select fplatformfsk,
                     fplatformname,
                     fgame_id,
                     fpname,
                     fuid,
                     play_days,
                     play_duration,
                     play_innings,
                     match_duration,
                     match_innings,
                     sum(play_duration) over(partition by fplatformfsk, fgame_id order by play_duration desc nulls last rows between unbounded preceding and current row) as play_duration_position,
                     sum(play_innings) over(partition by fplatformfsk, fgame_id order by play_innings desc nulls last rows between unbounded preceding and current row) as play_innings_position,
                     sum(match_duration) over(partition by fplatformfsk, fgame_id order by match_duration desc nulls last rows between unbounded preceding and current row) as match_duration_position,
                     sum(match_innings) over(partition by fplatformfsk, fgame_id order by match_innings desc nulls last rows between unbounded preceding and current row) as match_innings_position,
                     sum(play_duration) over(partition by fplatformfsk, fgame_id) as play_duration_area,
                     sum(play_innings) over(partition by fplatformfsk, fgame_id) as play_innings_area,
                     sum(match_duration) over(partition by fplatformfsk, fgame_id) as match_duration_area,
                     sum(match_innings) over(partition by fplatformfsk, fgame_id) as match_innings_area
                from (select fplatformfsk,
                             fplatformname,
                             fgame_id,
                             fpname,
                             fuid,
                             count(distinct dt) as play_days,
                             sum(play_duration) as play_duration,
                             sum(play_innings) as play_innings,
                             sum(match_duration) as match_duration,
                             sum(match_innings) as match_innings
                        from veda.dfqp_subgame_user_label_day
                       where dt >= date_sub('%(statdate)s', 6)
                         and dt <= '%(statdate)s'
                       group by fplatformfsk,
                                fplatformname,
                                fgame_id,
                                fpname,
                                fuid) m)
            insert overwrite table veda.dfqp_subgame_user_label_7day partition (dt = '%(statdate)s')
            select fplatformfsk,
                   fplatformname,
                   fgame_id,
                   fpname,
                   fuid,
                   coalesce(100 -
                            floor(play_duration_position * 50 / play_duration_area +
                                  play_innings_position * 50 / play_innings_area),
                            0) as play_passion,
                   cast(null as string) as play_label,
                   play_days,
                   play_duration,
                   play_innings,
                   coalesce(100 -
                            floor(match_duration_position * 50 / match_duration_area +
                                  match_innings_position * 50 / match_innings_area),
                            0) as match_passion,
                   cast(null as string) as match_label,
                   match_duration,
                   match_innings
              from label_data_7day
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            with label_data_30day as
             (select fplatformfsk,
                     fplatformname,
                     fgame_id,
                     fpname,
                     fuid,
                     play_days,
                     play_duration,
                     play_innings,
                     match_duration,
                     match_innings,
                     sum(play_duration) over(partition by fplatformfsk, fgame_id order by play_duration desc nulls last rows between unbounded preceding and current row) as play_duration_position,
                     sum(play_innings) over(partition by fplatformfsk, fgame_id order by play_innings desc nulls last rows between unbounded preceding and current row) as play_innings_position,
                     sum(match_duration) over(partition by fplatformfsk, fgame_id order by match_duration desc nulls last rows between unbounded preceding and current row) as match_duration_position,
                     sum(match_innings) over(partition by fplatformfsk, fgame_id order by match_innings desc nulls last rows between unbounded preceding and current row) as match_innings_position,
                     sum(play_duration) over(partition by fplatformfsk, fgame_id) as play_duration_area,
                     sum(play_innings) over(partition by fplatformfsk, fgame_id) as play_innings_area,
                     sum(match_duration) over(partition by fplatformfsk, fgame_id) as match_duration_area,
                     sum(match_innings) over(partition by fplatformfsk, fgame_id) as match_innings_area
                from (select fplatformfsk,
                             fplatformname,
                             fgame_id,
                             fpname,
                             fuid,
                             count(distinct dt) as play_days,
                             sum(play_duration) as play_duration,
                             sum(play_innings) as play_innings,
                             sum(match_duration) as match_duration,
                             sum(match_innings) as match_innings
                        from veda.dfqp_subgame_user_label_day
                       where dt >= date_sub('%(statdate)s', 29)
                         and dt <= '%(statdate)s'
                       group by fplatformfsk,
                                fplatformname,
                                fgame_id,
                                fpname,
                                fuid) m)
            insert overwrite table veda.dfqp_subgame_user_label_30day partition (dt = '%(statdate)s')
            select fplatformfsk,
                   fplatformname,
                   fgame_id,
                   fpname,
                   fuid,
                   coalesce(100 -
                            floor(play_duration_position * 50 / play_duration_area +
                                  play_innings_position * 50 / play_innings_area),
                            0) as play_passion,
                   cast(null as string) as play_label,
                   play_days,
                   play_duration,
                   play_innings,
                   coalesce(100 -
                            floor(match_duration_position * 50 / match_duration_area +
                                  match_innings_position * 50 / match_innings_area),
                            0) as match_passion,
                   cast(null as string) as match_label,
                   match_duration,
                   match_innings
              from label_data_30day
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
a = UserPortraitLabel(sys.argv[1:])
a()