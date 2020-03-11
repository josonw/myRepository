#-*- coding: UTF-8 -*-
# Author：AnsenWen
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_gamecoins_frequent_patten(BaseStat):
  def create_tab(self):
    """
    基于金流变动关系，用户的频繁模式,
    fwinner_bpid：           赢较多用户bpid
    fwinner_uid：            赢较多用户uid
    floser_uid：             输较多用户uid
    fwin_rate：              两者间赢较多用户面对输较多用户时的胜率
    fwinner_win_gamecoins：  赢较多用户赢得总金币
    floser_lose_gamecoins：  输较多用户输掉金币
    fwin_confidence：        置信度
    flose_confidence：       置信度
    ftype：                  疑似作弊类型
    """
    hql = """
      use stage;
      create table if not exists stage.user_gamecoins_frequent_patten
      (
            fdate                   date,
            fwinner_bpid            varchar(100),
            fwinner_uid             int,
            floser_uid              int,
            fwin_rate               decimal(30, 6),
            fwinner_win_gamecoins   bigint,
            floser_lose_gamecoins   bigint,
            fwin_confidence         decimal(30, 6),
            flose_confidence        decimal(30, 6),
            ftype                   tinyint
      )
      partitioned by(dt date)
    """
    res = self.hq.exe_sql(hql)
    if res != 0:
        return res

    return res

  def stat(self):
    """ 重要部分，统计内容 """
    dates_dict = PublicFunc.date_define(self.stat_date)
    min_gc_confidence = 0.5
    min_gp_confidence = 0.5
    min_support_cnt = 100000
    min_rate = 0.66
    query = {"min_gc_confidence": min_gc_confidence,
             "min_gp_confidence": min_gp_confidence,
             "min_support_cnt": min_support_cnt,
             "min_rate": min_rate}
    query.update(dates_dict)

    res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict;""")
    if res != 0: return res

    """
    单个用户的支持度
    """
    hql = """
        drop table if exists stage.user_gamecoins_support_one_%(num_begin)s;
      create table stage.user_gamecoins_support_one_%(num_begin)s as
      select '%(ld_daybegin)s' fdate,
             fgamefsk,
             fplatformfsk,
             a.fuid,
             sum(case when a.fgamecoins > 0 then a.fgamecoins else 0 end) fwin_gamecoins,
             sum(case when a.fgamecoins < 0 then -1 * a.fgamecoins else 0 end) flose_gamecoins
        from stage.user_gameparty_stg a
       inner join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
       where a.dt = '%(ld_daybegin)s'
         and coalesce(fmatch_id, '0') = '0'
       group by fgamefsk, fplatformfsk, a.fuid
      having fwin_gamecoins >= %(min_support_cnt)s or flose_gamecoins >= %(min_support_cnt)s
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    """
    输赢金币的支持度
    """

    hql = """
      drop table if exists stage.user_gamecoins_support_two_%(num_begin)s;
      create table stage.user_gamecoins_support_two_%(num_begin)s as
      select '%(ld_daybegin)s' fdate,
             fgamefsk,
             fplatformfsk,
             fwinner_bpid,
             fwinner_uid,
             floser_uid,
             fwin_gamecoins,
             flose_gamecoins,
             fwin_gamecoins / (fwin_gamecoins + flose_gamecoins) fwin_rate
        from stage.gameparty_pairs_mid
       where dt = '%(ld_daybegin)s'
         and fwin_gamecoins / (fwin_gamecoins + flose_gamecoins) > %(min_rate)s
         and fwin_gamecoins >= %(min_support_cnt)s
         and fis_match = 0
       group by fgamefsk,
                fplatformfsk,
                fwinner_bpid,
                fwinner_uid,
                floser_uid,
                fwin_gamecoins,
                flose_gamecoins
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    """
    输赢金币的置信度
    """

    hql = """
      drop table if exists stage.user_gamecoins_confidence_two_%(num_begin)s;
      create table stage.user_gamecoins_confidence_two_%(num_begin)s as
      select '%(ld_daybegin)s' fdate,
             fwinner_bpid,
             fgamefsk,
             fplatformfsk,
             fwinner_uid,
             floser_uid,
             flose_gc_confidence,
             fwin_gc_confidence
        from (select a.fgamefsk,
                     a.fplatformfsk,
                     a.fwinner_bpid,
                     a.fwinner_uid,
                     a.floser_uid,
                     a.flose_gamecoins / b.flose_gamecoins flose_gc_confidence,
                     a.fwin_gamecoins / c.fwin_gamecoins fwin_gc_confidence
                from stage.user_gamecoins_support_two_%(num_begin)s a
               inner join stage.user_gamecoins_support_one_%(num_begin)s b
                  on a.fgamefsk = b.fgamefsk
                 and a.fplatformfsk = b.fplatformfsk
                 and a.floser_uid = b.fuid
               inner join stage.user_gamecoins_support_one_%(num_begin)s c
                  on a.fgamefsk = c.fgamefsk
                 and a.fplatformfsk = c.fplatformfsk
                 and a.fwinner_uid = c.fuid
              ) a
       where flose_gc_confidence >= %(min_gc_confidence)s or fwin_gc_confidence >= %(min_gc_confidence)s
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    """
    频繁出现的输赢金币
    """

    hql = """
      insert overwrite table stage.user_gamecoins_frequent_patten
      partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
               a.fwinner_bpid,
               a.fwinner_uid,
               a.floser_uid,
               b.fwin_rate,
               b.fwin_gamecoins,
               b.flose_gamecoins,
               a.fwin_gc_confidence,
               a.flose_gc_confidence,
               case
                  when flose_gc_confidence >= %(min_gc_confidence)s and fwin_gc_confidence >= %(min_gc_confidence)s then 3
                  when flose_gc_confidence >= %(min_gc_confidence)s and fwin_gc_confidence < %(min_gc_confidence)s then 1
                  when flose_gc_confidence < %(min_gc_confidence)s and fwin_gc_confidence >= %(min_gc_confidence)s then 2 end ftype
          from stage.user_gamecoins_confidence_two_%(num_begin)s a
         inner join stage.user_gamecoins_support_two_%(num_begin)s b
            on a.fgamefsk = b.fgamefsk
           and a.fplatformfsk = b.fplatformfsk
           and a.fwinner_uid = b.fwinner_uid
           and a.floser_uid = b.floser_uid
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    hql = """
      drop table if exists stage.user_gamecoins_support_one_%(num_begin)s;
      drop table if exists stage.user_gamecoins_support_two_%(num_begin)s;
      drop table if exists stage.user_gamecoins_confidence_one_%(num_begin)s;
      drop table if exists stage.user_gamecoins_confidence_two_%(num_begin)s;
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    return res

if __name__ == '__main__':
  #愉快的统计要开始啦
  statDate = get_stat_date()
  #生成统计实例
  a = agg_user_gamecoins_frequent_patten(statDate)
  a()
