#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_gameparty_cheating_pair_fct(BaseStat):
  def create_tab(self):
    """
    根据用户的金币变动和输赢关系的频繁模式，找出疑似作弊的对弈关系
    fwinner_bpid：           赢较多用户bpid
    fwinner_uid：            赢较多用户uid
    floser_uid：             输较多用户uid
    fwin_rate：              两者间赢较多用户面对输较多用户时的胜率
    fwin_confidence：        赢牌的置信度
    flose_confidence：       输牌的置信度
    fwin_inning：            赢的牌局数
    flose_inning：           输掉的牌局数
    fwin_gamecoins：         赢得的金币数
    flose_gamecoins：        输掉的金币数
    fcheating_type：         作弊类型
    ffound_type：            识别的类型，是基于金币，还是基于输赢局数
    frank：                  综合排序
    """
    hql = """
      create table if not exists analysis.user_gameparty_cheating_pair_fct
      (
           fdate                date,
           fgamefsk             bigint,
           fplatformfsk         bigint,
           fversionfsk          bigint,
           fwinner_uid          int,
           floser_uid           int,
           fwin_rate            decimal(30, 6),
           fwin_confidence      decimal(30, 6),
           flose_confidence     decimal(30, 6),
           fwin_inning          int,
           flose_inning         int,
           fwin_gamecoins       bigint,
           flose_gamecoins      bigint,
           fcheating_type       tinyint,
           ffound_type          varchar(32),
           frank                decimal(30, 2)
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

    res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
    if res != 0: return res

    hql = """
        insert overwrite table analysis.user_gameparty_cheating_pair_fct
        partition(dt='%(ld_daybegin)s')
        select fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fwinner_uid,
               floser_uid,
               fwin_rate,
               fwin_confidence,
               flose_confidence,
               fwin_inning,
               flose_inning,
               fwin_gamecoins,
               flose_gamecoins,
               ftype fcheating_type,
               'gameparty' ffound_type,
               (fwin_rank + flose_rank + fsupport_rank) / 3 frank
          from (select a.fdate,
                       a.fwinner_bpid,
                       a.fwinner_uid,
                       a.floser_uid,
                       a.fwin_rate,
                       a.flose_confidence,
                       a.fwin_confidence,
                       a.ftype,
                       c.fwin_inning,
                       c.flose_inning,
                       c.fwin_gamecoins,
                       c.flose_gamecoins,
                       row_number() over(partition by a.fwinner_bpid order by fwin_confidence desc) fwin_rank,
                       row_number() over(partition by a.fwinner_bpid order by flose_confidence desc) flose_rank,
                       row_number() over(partition by a.fwinner_bpid order by fsupport_cnt desc) fsupport_rank
                  from (select * from stage.user_gameparty_frequent_patten where dt = '%(ld_daybegin)s') a
                 inner join analysis.bpid_platform_game_ver_map b
                    on a.fwinner_bpid = b.fbpid
                 inner join stage.gameparty_pairs_mid c
                    on b.fgamefsk = c.fgamefsk
                   and b.fplatformfsk = c.fplatformfsk
                   and a.fwinner_uid = c.fwinner_uid
                   and a.floser_uid = c.floser_uid
                   and c.dt = '%(ld_daybegin)s') t1
        inner join analysis.bpid_platform_game_ver_map t2
                on t1.fwinner_bpid = t2.fbpid
        where fwin_gamecoins >= 100000

        union all

        select fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fwinner_uid,
               floser_uid,
               fwin_rate,
               fwin_confidence,
               flose_confidence,
               fwin_inning,
               flose_inning,
               fwin_gamecoins,
               flose_gamecoins,
               ftype fcheating_type,
               'gamecoins' ffound_type,
               (fwin_rank + flose_rank + fsupport_rank) / 3 frank
          from (select a.fdate,
                       a.fwinner_bpid,
                       a.fwinner_uid,
                       a.floser_uid,
                       a.fwin_rate,
                       a.flose_confidence,
                       a.fwin_confidence,
                       a.ftype,
                       c.fwin_inning,
                       c.flose_inning,
                       c.fwin_gamecoins,
                       c.flose_gamecoins,
                       row_number() over(partition by a.fwinner_bpid order by a.fwin_confidence desc) fwin_rank,
                       row_number() over(partition by a.fwinner_bpid order by a.flose_confidence desc) flose_rank,
                       row_number() over(partition by a.fwinner_bpid order by c.fwin_gamecoins desc) fsupport_rank
                  from (select * from stage.user_gamecoins_frequent_patten where dt = '%(ld_daybegin)s') a
                 inner join analysis.bpid_platform_game_ver_map b
                    on a.fwinner_bpid = b.fbpid
                 inner join stage.gameparty_pairs_mid c
                    on b.fgamefsk = c.fgamefsk
                   and b.fplatformfsk = c.fplatformfsk
                   and a.fwinner_uid = c.fwinner_uid
                   and a.floser_uid = c.floser_uid
                   and c.dt = '%(ld_daybegin)s') t1
        inner join analysis.bpid_platform_game_ver_map t2
                on t1.fwinner_bpid = t2.fbpid
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    return res

if __name__ == '__main__':
  #愉快的统计要开始啦
  statDate = get_stat_date()
  #生成统计实例
  a = agg_user_gameparty_cheating_pair_fct(statDate)
  a()
