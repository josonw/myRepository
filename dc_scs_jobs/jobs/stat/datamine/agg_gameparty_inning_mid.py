#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_gameparty_inning_mid(BaseStat):
  def create_tab(self):
    """"
    建表，统计同一平台下每一牌局的汇总信息
    ftbl_id：                桌子ID
    finning_id：             牌局ID
    fis_match：              是否比赛
    fwin_gamecoins：         该牌局赢家赢得的总金币
    flose_gamecoins：        该牌局输家输掉的总金币
    fgamecoins：             牌局变动的金币，按理说赢得金币等于输掉金币，但特殊情况下，取两者较大
    fplayer_cnt：            牌局玩家人数
    fwin_player_cnt：        赢家人数
    flose_player_cnt：       输家人数
    fwin_max_uid：           赢得最多的玩家ID
    flose_max_uid：          输掉最多的玩家ID
    fwin_max_gamecoins：     赢得最多的玩家赢得的金币数
    flose_max_gamecoins：    输掉最多的玩家输掉的金币数
    """
    hql = """
      create table if not exists stage.gameparty_inning_mid
      (
        fdate               date,
        fgamefsk            bigint,
        fplatformfsk        bigint,
        ftbl_id             varchar(64),
        finning_id          varchar(64),
        fis_match           tinyint,
        fwin_gamecoins      bigint,
        flose_gamecoins     bigint,
        fgamecoins          bigint,
        fplayer_cnt         int,
        fwin_player_cnt     int,
        flose_player_cnt    int,
        fwin_max_uid        int,
        flose_max_uid       int,
        fwin_max_gamecoins  bigint,
        flose_max_gamecoins bigint
      )
      partitioned by(dt date)
    """
    res = self.hq.exe_sql(hql)
    if res != 0:
        return res

    return res

  def stat(self):
    """ 重要部分，统计内容 """
    #调试的时候把调试开关打开，正式执行的时候设置为0
    #self.hq.debug = 0
    dates_dict = PublicFunc.date_define(self.stat_date)
    query = {}
    query.update(dates_dict)

    res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict;""")
    if res != 0: return res

    hql = """
        drop table if exists stage.gameparty_inning_mid_tmp_01_%(num_begin)s;
        create table if not exists stage.gameparty_inning_mid_tmp_01_%(num_begin)s as
        select fdate,
               fgamefsk,
               fplatformfsk,
               ftbl_id,
               finning_id,
               fis_match,
               sum(case when fgamecoins > 0 then 1 * fgamecoins else 0 end) fwin_gamecoins,
               sum(case when fgamecoins < 0 then -1 * fgamecoins else 0 end) flose_gamecoins,
               count(fuid) fplayer_cnt,
               sum(case when fgamecoins >= 0 then 1 else 0 end) fwin_player_cnt,
               sum(case when fgamecoins < 0 then 1 else 0 end) flose_player_cnt
          from (
                  select to_date(flts_at) fdate,
                         fbpid,
                         fuid,
                         ftbl_id,
                         finning_id,
                         case when coalesce(fmatch_id, '0') = '0' then 0 else 1 end fis_match,
                         fgamecoins
                    from stage.user_gameparty_stg
                   where dt = '%(ld_daybegin)s'
              ) a
          join analysis.bpid_platform_game_ver_map b
            on a.fbpid = b.fbpid
         group by fdate, ftbl_id, finning_id, fgamefsk, fplatformfsk, fis_match
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    hql = """
        drop table if exists stage.gameparty_inning_mid_tmp_02_%(num_begin)s;
        create table stage.gameparty_inning_mid_tmp_02_%(num_begin)s as
        select fdate,
               fgamefsk,
               fplatformfsk,
               ftbl_id,
               finning_id,
               fuid fwin_max_uid,
               fgamecoins fwin_max_gamecoins
          from (select to_date(flts_at) fdate,
                       fgamefsk,
                       fplatformfsk,
                       ftbl_id,
                       finning_id,
                       fuid,
                       fgamecoins,
                       row_number() over(partition by fgamefsk, fplatformfsk, ftbl_id, finning_id order by fgamecoins) frnum
                  from stage.user_gameparty_stg a
                  join analysis.bpid_platform_game_ver_map b
                    on a.fbpid = b.fbpid
                 where dt = '%(ld_daybegin)s') t1
         where frnum = 1
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    hql = """
        drop table if exists stage.gameparty_inning_mid_tmp_03_%(num_begin)s;
        create table stage.gameparty_inning_mid_tmp_03_%(num_begin)s as
        select fdate,
               fgamefsk,
               fplatformfsk,
               ftbl_id,
               finning_id,
               fuid flose_max_uid,
               fgamecoins flose_max_gamecoins
          from (select to_date(flts_at) fdate,
                       fgamefsk,
                       fplatformfsk,
                       ftbl_id,
                       finning_id,
                       fuid,
                       fgamecoins,
                       row_number() over(partition by fgamefsk, fplatformfsk, ftbl_id, finning_id order by fgamecoins desc) frnum
                  from stage.user_gameparty_stg a
                  join analysis.bpid_platform_game_ver_map b
                    on a.fbpid = b.fbpid
                 where dt = '%(ld_daybegin)s') t1
         where frnum = 1
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    hql = """
      insert overwrite table stage.gameparty_inning_mid
      partition(dt='%(ld_daybegin)s')
      select a.fdate,
             a.fgamefsk,
             a.fplatformfsk,
             a.ftbl_id,
             a.finning_id,
             a.fis_match,
             fwin_gamecoins,
             flose_gamecoins,
             case when fwin_gamecoins >= flose_gamecoins then fwin_gamecoins else flose_gamecoins end fgamecoins,
             fplayer_cnt,
             fwin_player_cnt,
             flose_player_cnt,
             fwin_max_uid,
             flose_max_uid,
             fwin_max_gamecoins,
             flose_max_gamecoins
        from stage.gameparty_inning_mid_tmp_01_%(num_begin)s a
        join stage.gameparty_inning_mid_tmp_02_%(num_begin)s b
          on a.fgamefsk = b.fgamefsk
         and a.fplatformfsk = b.fplatformfsk
         and a.ftbl_id = b.ftbl_id
         and a.finning_id = b.finning_id
        join stage.gameparty_inning_mid_tmp_03_%(num_begin)s c
          on a.fgamefsk = c.fgamefsk
         and a.fplatformfsk = c.fplatformfsk
         and a.ftbl_id = c.ftbl_id
         and a.finning_id = c.finning_id
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    hql = """
      drop table if exists stage.gameparty_inning_mid_tmp_01_%(num_begin)s;
      drop table if exists stage.gameparty_inning_mid_tmp_02_%(num_begin)s;
      drop table if exists stage.gameparty_inning_mid_tmp_03_%(num_begin)s;
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    return res

if __name__ == '__main__':
  #愉快的统计要开始啦
  statDate = get_stat_date()
  #生成统计实例
  a = agg_gameparty_inning_mid(statDate)
  a()
