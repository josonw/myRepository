# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_gameparty_settlement(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.gameparty_settlement (
               fdate                  date,
               fgamefsk               bigint,
               fplatformfsk           bigint,
               fhallfsk               bigint,
               fsubgamefsk            bigint,
               fterminaltypefsk       bigint,
               fversionfsk            bigint,
               fchannelcode           bigint,
               fante                  bigint         comment '底注',
               fpname                 string         comment '牌局场次分类',
               fsubname               string         comment '牌局二级分类',
               fplay_unum             bigint         comment '玩牌人数',
               fbank_unum             bigint         comment '破产人数',
               fparty_num             bigint         comment '牌局场次',
               fwinplayer_cnt         bigint         comment '赢牌人数',
               floseplayer_cnt        bigint         comment '输牌人数',
               fbankparty_num         bigint         comment '破产场次',
               fwingc_sum             bigint         comment '赢游戏币数',
               flosegc_sum            bigint         comment '输游戏币数',
               fwingc_avg             bigint         comment '场均赢游戏币数',
               flosegc_avg            bigint         comment '场均输游戏币数',
               fcharge                bigint         comment '台费',
               fmultiple_avg          bigint         comment '场均倍数',
               f1bankrupt_num         bigint         comment '单杀场次',
               f2bankrupt_num         bigint         comment '双杀场次',
               fbankuser_cnt          bigint         comment '破产次数',
               frb_num                bigint         comment '机器人数量',
               frb_win_coins          bigint         comment '机器人发放游戏币',
               frb_lost_coins         bigint         comment '机器人消耗游戏币',
               frb_win_party          bigint         comment '机器人赢牌的场次',
               frb_party              bigint         comment '机器人参与的场次',
               fbankpay_unum          bigint         comment '破产后30分钟付费人数',
               fbankusercnt           bigint         comment '破产人数(计算破产付费率的分母)',
               f10bankpay_unum        bigint         comment '破产后10分钟付费人数'
               )comment '牌局日报_牌局结算'
               partitioned by(dt date)
        location '/dw/dcnew/gameparty_settlement'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':['fante','fpname','fsubname'],
                        'groups':[[1,1,1],
                                  [0,1,1],
                                  [1,1,0]] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--取牌局结算相关数据
        drop table if exists work.gp_settlement_tmp_1_%(statdatenum)s;
        create table work.gp_settlement_tmp_1_%(statdatenum)s as
                    select c.fgamefsk,
                           c.fplatformfsk,
                           c.fhallfsk,
                           c.fterminaltypefsk,
                           c.fversionfsk,
                           c.hallmode,
                           coalesce(a.fgame_id,cast (0 as bigint)) fgame_id,
                           coalesce(cast (a.fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                           coalesce(a.fante,%(null_int_report)d) fante,
                           coalesce(a.fpname,'%(null_str_report)s') fpname,
                           coalesce(a.fsubname,'%(null_str_report)s') fsubname,
                           a.fwin_player_cnt, --赢牌人数
                           a.flose_player_cnt, --输牌人数
                           a.fbankrupt_num , --破产人数
                           a.fwin_gc_num, --赢游戏币数
                           a.flose_gc_num, --输游戏币数
                           a.fcharge, --台费
                           a.fmultiple, --场均倍数
                           a.frobots_num, --机器人数量
                           a.frobots_gcin_num, --机器人发放游戏币
                           a.frobots_gcout_num,--机器人消耗游戏币
                           a.fwin_robots_num, --赢的机器人数量
                           a.flose_robots_num --输的机器人数量
                      from stage.gameparty_settlement_stg a
                      join dim.bpid_map c
                        on a.fbpid=c.fbpid
                     where a.dt = "%(statdate)s"
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--取牌局人数数据
        drop table if exists work.gp_settlement_tmp_2_%(statdatenum)s;
        create table work.gp_settlement_tmp_2_%(statdatenum)s as
                    select distinct c.fgamefsk,
                           c.fplatformfsk,
                           c.fhallfsk,
                           c.fterminaltypefsk,
                           c.fversionfsk,
                           c.hallmode,
                           coalesce(a.fgame_id,cast (0 as bigint)) fgame_id,
                           coalesce(cast (a.fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                           coalesce(a.fblind_1,%(null_int_report)d) fante,
                           coalesce(a.fpname,'%(null_str_report)s') fpname,
                           coalesce(a.fsubname,'%(null_str_report)s') fsubname,
                           a.fuid  --玩牌人数
                      from stage.user_gameparty_stg a
                      join dim.bpid_map c
                        on a.fbpid=c.fbpid
                     where a.dt = "%(statdate)s"
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--取破产及破产付费数据
        drop table if exists work.gp_settlement_tmp_3_%(statdatenum)s;
        create table work.gp_settlement_tmp_3_%(statdatenum)s as
        select d.fgamefsk,
               d.fplatformfsk,
               d.fhallfsk,
               d.fterminaltypefsk,
               d.fversionfsk,
               d.hallmode,
               fgame_id,
               fchannel_code,
               fante,
               fpname,
               fsubname,
               fuid,
               fbankpay,
               f10bankpay
          from (select fbpid,
                       fgame_id,
                       fchannel_code,
                       fante,
                       fpname,
                       fsubname,
                       fuid,
                       fbankpay,
                       f10bankpay,
                       row_number() over(partition by fbpid,fgame_id,fchannel_code,fante,fpname,fsubname,fuid
                                         order by f10bankpay desc,fbankpay desc ) row_num
                  from (select a.fbpid,
                               coalesce(a.fgame_id,cast (0 as bigint)) fgame_id,
                               coalesce(cast (a.fchannel_code as bigint),%(null_int_report)d) fchannel_code,
                               coalesce(a.fuphill_pouring,%(null_int_report)d) fante,
                               coalesce(a.fpname,'%(null_str_report)s') fpname,
                               coalesce(a.fplayground_title,'%(null_str_report)s') fsubname,
                               a.fuid,
                               case when unix_timestamp(c.fdate)-1800 <= unix_timestamp(a.frupt_at) then 1 else 0 end fbankpay,
                               case when unix_timestamp(c.fdate)-600 <= unix_timestamp(a.frupt_at) then 1 else 0 end f10bankpay
                          from stage.user_bankrupt_stg a
                          left join dim.user_pay b
                            on a.fbpid = b.fbpid
                           and a.fuid = b.fuid
                          left join stage.payment_stream_stg c
                            on b.fplatform_uid = c.fplatform_uid
                           and c.dt="%(statdate)s"
                         where a.dt="%(statdate)s"
                       ) t
               ) a
          join dim.bpid_map  d
            on a.fbpid=d.fbpid
         where row_num = 1
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--组合牌局结算相关数据
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       coalesce(fante,%(null_int_group_rule)d) fante,
                       coalesce(fpname,'%(null_str_group_rule)s') fpname,
                       coalesce(fsubname,'%(null_str_group_rule)s') fsubname,
                       count(1) fparty_num, --牌局场次
                       sum(a.fwin_player_cnt) fwinplayer_cnt, --赢牌人数
                       sum(a.flose_player_cnt) floseplayer_cnt, --输牌人数
                       count(case when a.fbankrupt_num > 0 then 1 end) fbankparty_num, --破产场次
                       sum(a.fwin_gc_num) fwingc_sum, --赢游戏币数
                       sum(a.flose_gc_num) flosegc_sum, --输游戏币数
                       round(avg(a.fwin_gc_num)) fwingc_avg, --场均赢游戏币数
                       round(avg(a.flose_gc_num)) flosegc_avg, --场均输游戏币数
                       sum(a.fcharge) fcharge, --台费
                       round(avg(a.fmultiple)) fmultiple_avg, --场均倍数
                       count(case when a.fbankrupt_num=1 then 1 else null end) f1bankrupt_num, --单杀场次
                       count(case when a.fbankrupt_num=2 then 1 else null end) f2bankrupt_num, --双杀场次
                       sum(a.frobots_num) frb_num, --机器人数量
                       sum(abs(a.frobots_gcin_num)) frb_win_coins, --机器人发放游戏币
                       sum(abs(a.frobots_gcout_num)) frb_lost_coins, --机器人消耗游戏币
                       count(case when a.frobots_num > 0 and a.fwin_robots_num > 0 then 1 end) frb_win_party, --机器人赢牌的场次
                       count(case when a.frobots_num > 0 then 1 end) frb_party --机器人参与的场次
                  from work.gp_settlement_tmp_1_%(statdatenum)s a
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fante,
                       fpname,
                       fsubname

        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.gp_settlement_tmp_zh_1_%(statdatenum)s;
        create table work.gp_settlement_tmp_zh_1_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--组合牌局人数数据
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       coalesce(fante,%(null_int_group_rule)d) fante,
                       coalesce(fpname,'%(null_str_group_rule)s') fpname,
                       coalesce(fsubname,'%(null_str_group_rule)s') fsubname,
                       count(distinct a.fuid) fplay_unum --玩牌人数
                  from work.gp_settlement_tmp_2_%(statdatenum)s a
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fante,
                       fpname,
                       fsubname

        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.gp_settlement_tmp_zh_2_%(statdatenum)s;
        create table work.gp_settlement_tmp_zh_2_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--组合破产及破产付费数据
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       coalesce(fante,%(null_int_group_rule)d) fante,
                       coalesce(fpname,'%(null_str_group_rule)s') fpname,
                       coalesce(fsubname,'%(null_str_group_rule)s') fsubname,
                       count(distinct a.fuid) fbank_unum, --破产人数
                       0 fbankuser_cnt, --破产次数
                       count(distinct case when fbankpay = 1 then a.fuid end) fbankpay_unum, --破产后30分钟付费人数
                       count(distinct a.fuid) fbankusercnt, --破产人数(计算破产付费率的分母)
                       count(distinct case when f10bankpay = 1 then a.fuid end) f10bankpay_unum  --破产后10分钟付费人数
                  from work.gp_settlement_tmp_3_%(statdatenum)s a
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fante,
                       fpname,
                       fsubname

        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.gp_settlement_tmp_zh_3_%(statdatenum)s;
        create table work.gp_settlement_tmp_zh_3_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--取破产次数数据
        drop table if exists work.gp_settlement_tmp_4_%(statdatenum)s;
        create table work.gp_settlement_tmp_4_%(statdatenum)s as
        select d.fgamefsk,
               d.fplatformfsk,
               d.fhallfsk,
               d.fterminaltypefsk,
               d.fversionfsk,
               d.hallmode,
               coalesce(a.fgame_id,cast (0 as bigint)) fgame_id,
               coalesce(cast (a.fchannel_code as bigint),%(null_int_report)d) fchannel_code,
               coalesce(a.fuphill_pouring,%(null_int_report)d) fante,
               coalesce(a.fpname,'%(null_str_report)s') fpname,
               coalesce(a.fplayground_title,'%(null_str_report)s') fsubname,
               a.fuid
          from stage.user_bankrupt_stg a
          join dim.bpid_map  d
            on a.fbpid=d.fbpid
         where a.dt="%(statdate)s"
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--组合破产次数数据
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       coalesce(fante,%(null_int_group_rule)d) fante,
                       coalesce(fpname,'%(null_str_group_rule)s') fpname,
                       coalesce(fsubname,'%(null_str_group_rule)s') fsubname,
                       0 fbank_unum, --破产人数
                       count(fuid) fbankuser_cnt, --破产次数
                       0 fbankpay_unum, --破产后30分钟付费人数
                       0 fbankusercnt, --破产人数(计算破产付费率的分母)
                       0 f10bankpay_unum  --破产后10分钟付费人数
                  from work.gp_settlement_tmp_4_%(statdatenum)s
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fante,
                       fpname,
                       fsubname

        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert into work.gp_settlement_tmp_zh_3_%(statdatenum)s
        %(sql_template)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--汇总目标表
        insert overwrite table dcnew.gameparty_settlement
        partition( dt="%(statdate)s" )
                select "%(statdate)s" fdate,
                       fgamefsk,
                       fplatformfsk,
                       fhallfsk,
                       fgame_id,
                       fterminaltypefsk,
                       fversionfsk,
                       fchannel_code,
                       fante,
                       fpname,
                       fsubname,
                       sum(fplay_unum) fplay_unum, --玩牌人数
                       sum(fbank_unum) fbank_unum, --破产人数
                       sum(fparty_num) fparty_num, --牌局场次
                       sum(fwinplayer_cnt) fwinplayer_cnt, --赢牌人数
                       sum(floseplayer_cnt) floseplayer_cnt, --输牌人数
                       sum(fbankparty_num) fbankparty_num, --破产场次
                       sum(fwingc_sum) fwingc_sum, --赢游戏币数
                       sum(flosegc_sum) flosegc_sum, --输游戏币数
                       sum(fwingc_avg) fwingc_avg, --场均赢游戏币数
                       sum(flosegc_avg) flosegc_avg, --场均输游戏币数
                       sum(fcharge) fcharge, --台费
                       sum(fmultiple_avg) fmultiple_avg, --场均倍数
                       sum(f1bankrupt_num) f1bankrupt_num, --单杀场次
                       sum(f2bankrupt_num) f2bankrupt_num, --双杀场次
                       sum(fbankuser_cnt) fbankuser_cnt, --破产次数
                       sum(frb_num) frb_num, --机器人数量
                       sum(frb_win_coins) frb_win_coins, --机器人发放游戏币
                       sum(frb_lost_coins) frb_lost_coins, --机器人消耗游戏币
                       sum(frb_win_party) frb_win_party, --机器人赢牌的场次
                       sum(frb_party) frb_party, --机器人参与的场次
                       sum(fbankpay_unum) fbankpay_unum, --破产后30分钟付费人数
                       sum(fbankusercnt) fbankusercnt, --破产人数(计算破产付费率的分母)
                       sum(f10bankpay_unum) f10bankpay_unum --破产后10分钟付费人数
                  from ( select fgamefsk,
                                fplatformfsk,
                                fhallfsk,
                                fterminaltypefsk,
                                fversionfsk,
                                fgame_id,
                                fchannel_code,
                                fante,
                                fpname,
                                fsubname,
                                0 fplay_unum, --玩牌人数
                                0 fbank_unum, --破产人数
                                fparty_num, --牌局场次
                                fwinplayer_cnt, --赢牌人数
                                floseplayer_cnt, --输牌人数
                                fbankparty_num, --破产场次
                                fwingc_sum, --赢游戏币数
                                flosegc_sum, --输游戏币数
                                fwingc_avg, --场均赢游戏币数
                                flosegc_avg, --场均输游戏币数
                                fcharge, --台费
                                fmultiple_avg, --场均倍数
                                f1bankrupt_num, --单杀场次
                                f2bankrupt_num, --双杀场次
                                0 fbankuser_cnt, --破产次数
                                frb_num, --机器人数量
                                frb_win_coins, --机器人发放游戏币
                                frb_lost_coins, --机器人消耗游戏币
                                frb_win_party, --机器人赢牌的场次
                                frb_party, --机器人参与的场次
                                0 fbankpay_unum, --破产后30分钟付费人数
                                0 fbankusercnt, --破产人数(计算破产付费率的分母)
                                0 f10bankpay_unum --破产后10分钟付费人数
                           from work.gp_settlement_tmp_zh_1_%(statdatenum)s
                          union all
                         select fgamefsk,
                                fplatformfsk,
                                fhallfsk,
                                fterminaltypefsk,
                                fversionfsk,
                                fgame_id,
                                fchannel_code,
                                fante,
                                fpname,
                                fsubname,
                                fplay_unum, --玩牌人数
                                0 fbank_unum, --破产人数
                                0 fparty_num, --牌局场次
                                0 fwinplayer_cnt, --赢牌人数
                                0 floseplayer_cnt, --输牌人数
                                0 fbankparty_num, --破产场次
                                0 fwingc_sum, --赢游戏币数
                                0 flosegc_sum, --输游戏币数
                                0 fwingc_avg, --场均赢游戏币数
                                0 flosegc_avg, --场均输游戏币数
                                0 fcharge, --台费
                                0 fmultiple_avg, --场均倍数
                                0 f1bankrupt_num, --单杀场次
                                0 f2bankrupt_num, --双杀场次
                                0 fbankuser_cnt, --破产次数
                                0 frb_num, --机器人数量
                                0 frb_win_coins, --机器人发放游戏币
                                0 frb_lost_coins, --机器人消耗游戏币
                                0 frb_win_party, --机器人赢牌的场次
                                0 frb_party, --机器人参与的场次
                                0 fbankpay_unum, --破产后30分钟付费人数
                                0 fbankusercnt, --破产人数(计算破产付费率的分母)
                                0 f10bankpay_unum --破产后10分钟付费人数
                           from work.gp_settlement_tmp_zh_2_%(statdatenum)s
                          union all
                         select fgamefsk,
                                fplatformfsk,
                                fhallfsk,
                                fterminaltypefsk,
                                fversionfsk,
                                fgame_id,
                                fchannel_code,
                                fante,
                                fpname,
                                fsubname,
                                0 fplay_unum, --玩牌人数
                                fbank_unum, --破产人数
                                0 fparty_num, --牌局场次
                                0 fwinplayer_cnt, --赢牌人数
                                0 floseplayer_cnt, --输牌人数
                                0 fbankparty_num, --破产场次
                                0 fwingc_sum, --赢游戏币数
                                0 flosegc_sum, --输游戏币数
                                0 fwingc_avg, --场均赢游戏币数
                                0 flosegc_avg, --场均输游戏币数
                                0 fcharge, --台费
                                0 fmultiple_avg, --场均倍数
                                0 f1bankrupt_num, --单杀场次
                                0 f2bankrupt_num, --双杀场次
                                fbankuser_cnt, --破产次数
                                0 frb_num, --机器人数量
                                0 frb_win_coins, --机器人发放游戏币
                                0 frb_lost_coins, --机器人消耗游戏币
                                0 frb_win_party, --机器人赢牌的场次
                                0 frb_party, --机器人参与的场次
                                fbankpay_unum, --破产后30分钟付费人数
                                fbankusercnt, --破产人数(计算破产付费率的分母)
                                f10bankpay_unum --破产后10分钟付费人数
                           from work.gp_settlement_tmp_zh_3_%(statdatenum)s
                       ) t
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fante,
                       fpname,
                       fsubname

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.gp_settlement_tmp_1_%(statdatenum)s;
                 drop table if exists work.gp_settlement_tmp_2_%(statdatenum)s;
                 drop table if exists work.gp_settlement_tmp_3_%(statdatenum)s;
                 drop table if exists work.gp_settlement_tmp_4_%(statdatenum)s;
                 drop table if exists work.gp_settlement_tmp_zh_1_%(statdatenum)s;
                 drop table if exists work.gp_settlement_tmp_zh_2_%(statdatenum)s;
                 drop table if exists work.gp_settlement_tmp_zh_3_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return resS
        return res


#生成统计实例
a = agg_gameparty_settlement(sys.argv[1:])
a()
