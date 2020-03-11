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


class agg_dfqp_subgame_portrait_match(BaseStatModel):
    """地方棋牌比赛画像数据"""
    def create_tab(self):
        hql = """
            create table if not exists veda.dfqp_game_portrait_match
            (
             game_id int comment '子游戏ID', 
             game_name string comment '子游戏名称', 
             match_players bigint comment '比赛人数(去重)', 
             match_new_enter_players bigint comment '比赛新进入人数(去重)', 
             match_new_enter_players_newregistration bigint comment '比赛新进入人数(去重)-新注册用户', 
             match_new_enter_players_registered bigint comment '比赛新进入人数(去重)-老用户', 
             match_new_players bigint comment '比赛新增人数(去重)', 
             match_new_players_newregistration bigint comment '比赛新增人数(去重)-新注册用户', 
             match_new_players_registered bigint comment '比赛新增人数(去重)-老用户', 
             match_entry_fee bigint comment '比赛报名费', 
             match_apply_cnt bigint comment '比赛报名次数', 
             match_inning_num bigint comment '比赛局数', 
             match_cnt bigint comment '比赛人次', 
             match_duration bigint comment '比赛时长', 
             match_quit_cnt bigint comment '比赛取消报名次数', 
             match_out_cost bigint comment '比赛发放成本', 
             match_in_cost bigint comment '比赛消耗成本', 
             match_gold_out bigint comment '比赛金条产出', 
             match_gold_in bigint comment '比赛金条回收', 
             match_bill_out bigint comment '比赛发放话费', 
             match_relieve_num bigint comment '比赛复活次数', 
             match_duration_inning_avg bigint comment '平均每局比赛时长', 
             match_num_avg bigint comment '平均每人比赛局数', 
             match_duration_player_avg bigint comment '平均每人比赛时长'
            ) comment '地方棋牌比赛（到子游戏）画像数据'
            partitioned by (dt string);


            create table if not exists veda.dfqp_subgame_portrait_match
            (
             game_id int comment '子游戏ID', 
             game_name string comment '子游戏名称', 
             subname string comment '比赛三级场次', 
             hallname string comment '大厅名称',
             match_players bigint comment '比赛人数(去重)', 
             match_new_enter_players bigint comment '比赛新进入人数(去重)', 
             match_new_enter_players_newregistration bigint comment '比赛新进入人数(去重)-新注册用户', 
             match_new_enter_players_registered bigint comment '比赛新进入人数(去重)-老用户', 
             match_new_players bigint comment '比赛新增人数(去重)', 
             match_new_players_newregistration bigint comment '比赛新增人数(去重)-新注册用户', 
             match_new_players_registered bigint comment '比赛新增人数(去重)-老用户', 
             match_entry_fee bigint comment '比赛报名费', 
             match_apply_cnt bigint comment '比赛报名次数', 
             match_inning_num bigint comment '比赛局数', 
             match_cnt bigint comment '比赛人次', 
             match_duration bigint comment '比赛时长', 
             match_quit_cnt bigint comment '比赛取消报名次数', 
             match_out_cost bigint comment '比赛发放成本', 
             match_in_cost bigint comment '比赛消耗成本', 
             match_gold_out bigint comment '比赛金条产出', 
             match_gold_in bigint comment '比赛金条回收', 
             match_bill_out bigint comment '比赛发放话费', 
             match_relieve_num bigint comment '比赛复活次数', 
             match_duration_inning_avg bigint comment '平均每局比赛时长', 
             match_num_avg bigint comment '平均每人比赛局数', 
             match_duration_player_avg bigint comment '平均每人比赛时长'
            ) comment '地方棋牌比赛（到场次）画像数据'
            partitioned by (dt string)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            insert overwrite table veda.dfqp_game_portrait_match
            partition (dt='%(statdate)s')
            select t1.game_id,
                   t1.game_name,
                   count(distinct t1.fuid) as match_players,
                   count(distinct(case
                                    when t2.new_enter = 'Y' and nvl(t1.if_newbie, 0) = 0 then
                                     t2.fuid
                                    else
                                     null
                                  end)) as match_new_enter_players,
                   count(distinct(case
                                    when t2.new_regedit = 'Y' and t2.new_enter = 'Y' and
                                         nvl(t1.if_newbie, 0) = 0 then
                                     t2.fuid
                                    else
                                     null
                                  end)) as match_new_enter_players_newregistration,
                   count(distinct(case
                                    when t2.new_regedit = 'N' and t2.new_enter = 'Y' and
                                         nvl(t1.if_newbie, 0) = 0 then
                                     t2.fuid
                                    else
                                     null
                                  end)) as match_new_enter_players_registered,
                   count(distinct(case
                                    when t1.if_newbie = 1 then
                                     t2.fuid
                                    else
                                     null
                                  end)) as match_new_players,
                   count(distinct(case
                                    when t2.new_regedit = 'Y' and t1.if_newbie = 1 then
                                     t1.fuid
                                    else
                                     null
                                  end)) as match_new_players_newregistration,
                   count(distinct(case
                                    when t2.new_regedit = 'N' and t1.if_newbie = 1 then
                                     t1.fuid
                                    else
                                     null
                                  end)) as match_new_players_registered,
                   sum(t1.fentry_fee) as match_entry_fee,
                   sum(t1.fapply_cnt) as match_apply_cnt,
                   sum(t1.fmatch_cnt) as match_inning_num,
                   count(t1.fuid) as match_cnt,
                   sum(match_duration) as match_duration,
                   sum(t1.fquit_cnt) match_quit_cnt,
                   sum(t1.fout_cost) match_out_cost,
                   sum(t1.fin_cost) match_in_cost,
                   sum(t1.fgold_out) match_gold_out,
                   sum(t1.fgold_in) match_gold_in,
                   sum(t1.fbill_out) match_bill_out,
                   sum(t1.frelieve_num) match_relieve_num,
                   sum(match_duration) / sum(t1.fmatch_cnt) as match_duration_inning_avg,
                   sum(t1.fmatch_cnt) / count(distinct t1.fuid) as match_num_avg,
                   sum(match_duration) / count(distinct t1.fuid) as match_duration_player_avg
              from (select m1.fgame_id       as game_id,
                           m1.fpname         as game_name,
                           m1.fuid,
                           m3.if_newbie,
                           m1.fentry_fee,
                           m1.fapply_cnt,
                           m1.fmatch_cnt,
                           m3.match_duration,
                           m1.fquit_cnt,
                           m1.fout_cost,
                           m1.fin_cost,
                           m1.fgold_out,
                           m1.fgold_in,
                           m1.fbill_out,
                           m1.frelieve_num,
                           m1.dt
                      from dim.bpid_map m0
                     inner join (select fbpid,
                                       fgame_id,
                                       fpname,
                                       fuid,
                                       max(ffirst_play) as if_newbie,
                                       sum(unix_timestamp(fe_timer) -
                                           unix_timestamp(fs_timer)) as match_duration,
                                       dt
                                  from dim.match_gameparty
                                 group by fbpid, fgame_id, fpname, fuid, dt) m3
                        on (m0.fbpid = m3.fbpid and m0.fgamename = '地方棋牌' and
                           m3.dt = '%(statdate)s')
                     inner join veda.user_match_cube m1
                        on (m1.fgame_id = m3.fgame_id and m1.fpname = m3.fpname and
                           m1.fuid = m3.fuid and m1.dt = m3.dt and
                           m3.dt = '%(statdate)s')) t1
              left join (select fuid, new_regedit, new_enter, dt
                           from stage_dfqp.dfqp_user_register_enter
                          where dt = '%(statdate)s') t2
                on (t1.fuid = t2.fuid)
             group by t1.game_id, t1.game_name;


             
            insert overwrite table veda.dfqp_subgame_portrait_match
            partition (dt='%(statdate)s')
            select t1.game_id,
                   t1.game_name,
                   t1.subname,
                   t1.hallname,
                   count(distinct t1.fuid) as match_players,
                   count(distinct(case
                                    when t2.new_enter = 'Y' and nvl(t1.if_newbie, 0) = 0 then
                                     t2.fuid
                                    else
                                     null
                                  end)) as match_new_enter_players,
                   count(distinct(case
                                    when t2.new_regedit = 'Y' and t2.new_enter = 'Y' and
                                         nvl(t1.if_newbie, 0) = 0 then
                                     t2.fuid
                                    else
                                     null
                                  end)) as match_new_enter_players_newregistration,
                   count(distinct(case
                                    when t2.new_regedit = 'N' and t2.new_enter = 'Y' and
                                         nvl(t1.if_newbie, 0) = 0 then
                                     t2.fuid
                                    else
                                     null
                                  end)) as match_new_enter_players_registered,
                   count(distinct(case
                                    when t1.if_newbie = 1 then
                                     t2.fuid
                                    else
                                     null
                                  end)) as match_new_players,
                   count(distinct(case
                                    when t2.new_regedit = 'Y' and t1.if_newbie = 1 then
                                     t1.fuid
                                    else
                                     null
                                  end)) as match_new_players_newregistration,
                   count(distinct(case
                                    when t2.new_regedit = 'N' and t1.if_newbie = 1 then
                                     t1.fuid
                                    else
                                     null
                                  end)) as match_new_players_registered,
                   sum(t1.fentry_fee) as match_entry_fee,
                   sum(t1.fapply_cnt) as match_apply_cnt,
                   sum(t1.fmatch_cnt) as match_inning_num,
                   count(t1.fuid) as match_cnt,
                   sum(match_duration) as match_duration,
                   sum(t1.fquit_cnt) match_quit_cnt,
                   sum(t1.fout_cost) match_out_cost,
                   sum(t1.fin_cost) match_in_cost,
                   sum(t1.fgold_out) match_gold_out,
                   sum(t1.fgold_in) match_gold_in,
                   sum(t1.fbill_out) match_bill_out,
                   sum(t1.frelieve_num) match_relieve_num,
                   sum(match_duration) /
                   sum(t1.fmatch_cnt) as match_duration_inning_avg,
                   sum(t1.fmatch_cnt) / count(distinct t1.fuid) as match_num_avg,
                   sum(match_duration) /
                   count(distinct t1.fuid) as match_duration_player_avg
              from (select m1.fgame_id       as game_id,
                           m1.fpname         as game_name,
                           m3.fgsubname      as subname,
                           m0.fhallname      as hallname,
                           m1.fuid,
                           m3.if_newbie,
                           m1.fentry_fee,
                           m1.fapply_cnt,
                           m1.fmatch_cnt,
                           m3.match_duration,
                           m1.fquit_cnt,
                           m1.fout_cost,
                           m1.fin_cost,
                           m1.fgold_out,
                           m1.fgold_in,
                           m1.fbill_out,
                           m1.frelieve_num,
                           m1.dt
                      from dim.bpid_map m0
                     inner join (select fbpid,
                                       fgame_id,
                                       fpname,
                                       fgsubname,
                                       fuid,
                                       max(ffirst_play) as if_newbie,
                                       sum(unix_timestamp(fe_timer) -
                                           unix_timestamp(fs_timer)) as match_duration,
                                       dt
                                  from dim.match_gameparty
                                 group by fbpid, fgame_id, fpname, fgsubname, fuid, dt) m3
                        on (m0.fbpid = m3.fbpid and m0.fgamename = '地方棋牌' and
                           m3.dt = '%(statdate)s')
                     inner join veda.user_match_cube m1
                        on (m1.fgame_id = m3.fgame_id and m1.fpname = m3.fpname and
                           m1.fgsubname = m3.fgsubname and m1.fuid = m3.fuid and
                           m1.dt = m3.dt and m3.dt = '%(statdate)s')) t1
              left join (select fuid, new_regedit, new_enter, dt
                           from stage_dfqp.dfqp_user_register_enter
                          where dt = '%(statdate)s') t2
                on (t1.fuid = t2.fuid)
             group by t1.game_id, t1.game_name, t1.subname, t1.hallname
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_dfqp_subgame_portrait_match(sys.argv[1:])
a()

