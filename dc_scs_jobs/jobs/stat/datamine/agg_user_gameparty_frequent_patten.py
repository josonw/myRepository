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

class agg_user_gameparty_frequent_patten(BaseStat):
  def create_tab(self):
    """
    基于牌局输赢关系，用户的频繁模式
    fwinner_bpid：           赢较多用户bpid
    fwinner_uid：            赢较多用户uid
    floser_uid：             输较多用户uid
    fsupport_cnt：           支持度，即两者间玩牌局数
    fwin_rate：              两者间赢较多用户面对输较多用户时的胜率
    fwin_confidence：        赢牌的置信度
    flose_confidence：       输牌的置信度
    ftype：                  疑似作弊类型
    """
    hql = """
      use stage;
      create table if not exists stage.user_gameparty_frequent_patten
      (
        fdate           date,
        fwinner_bpid   varchar(100),
        fwinner_uid     int,
        floser_uid      int,
        fsupport_cnt    decimal(30),
        fwin_rate       decimal(30, 6),
        fwin_confidence decimal(30, 6),
        flose_confidence decimal(30, 6),
        ftype           tinyint
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
    min_confidence = 0.5
    min_support = 0.00001
    min_support_cnt = 10
    min_rate = 0.66
    query = {"min_support": min_support,
             "min_confidence": min_confidence,
             "min_support_cnt": min_support_cnt,
             "min_rate": min_rate,}
    query.update(dates_dict)

    res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
    if res != 0: return res

    """
    单个用户的支持度
    """
    hql = """
      drop table if exists stage.user_gameparty_support_one_%(num_begin)s;
      create table stage.user_gameparty_support_one_%(num_begin)s as
      select '%(ld_daybegin)s' fdate,
             fgamefsk,
             fplatformfsk,
             a.fuid,
             count(distinct finning_id) fcnt
        from stage.user_gameparty_stg a
       inner join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
       where a.dt = '%(ld_daybegin)s'
         and coalesce(fmatch_id, '0') = '0'
       group by fgamefsk, fplatformfsk, a.fuid
       having count(distinct finning_id) >= %(min_support_cnt)s
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    """
    输赢关系的支持度
    """

    hql = """
      drop table if exists stage.user_gameparty_support_two_%(num_begin)s;
      create table stage.user_gameparty_support_two_%(num_begin)s as
      select '%(ld_daybegin)s' fdate,
             fgamefsk,
             fplatformfsk,
             fwinner_bpid,
             fwinner_uid,
             floser_uid,
             fwin_inning + flose_inning fsupport_cnt,
             fwin_inning / (fwin_inning + flose_inning) fwin_rate
        from stage.gameparty_pairs_mid
       where dt = '%(ld_daybegin)s'
         and fwin_inning / (fwin_inning + flose_inning) > %(min_rate)s
         and fwin_inning + flose_inning >= %(min_support_cnt)s
         and fis_match = 0
       group by fgamefsk,
                fplatformfsk,
                fwinner_bpid,
                fwinner_uid,
                floser_uid,
                fwin_inning,
                flose_inning
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    """
    输赢关系的置信度
    """

    hql = """
      drop table if exists stage.user_gameparty_confidence_two_%(num_begin)s;
      create table stage.user_gameparty_confidence_two_%(num_begin)s as
      select '%(ld_daybegin)s' fdate,
             fwinner_bpid,
             fgamefsk,
             fplatformfsk,
             fwinner_uid,
             floser_uid,
             flose_confidence,
             fwin_confidence
        from (select a.fwinner_bpid,
                     a.fgamefsk,
                     a.fplatformfsk,
                     a.fwinner_uid,
                     a.floser_uid,
                     a.fsupport_cnt / b.fcnt flose_confidence,
                     a.fsupport_cnt / c.fcnt fwin_confidence
                from stage.user_gameparty_support_two_%(num_begin)s a
               inner join stage.user_gameparty_support_one_%(num_begin)s b
                  on a.fgamefsk = b.fgamefsk
                 and a.fplatformfsk = b.fplatformfsk
                 and a.floser_uid = b.fuid
               inner join stage.user_gameparty_support_one_%(num_begin)s c
                  on a.fgamefsk = c.fgamefsk
                 and a.fplatformfsk = c.fplatformfsk
                 and a.fwinner_uid = c.fuid
              ) a
       where flose_confidence >= %(min_confidence)s
          or fwin_confidence >= %(min_confidence)s
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    """
    频繁出现的输赢关系
    """

    hql = """
      insert overwrite table stage.user_gameparty_frequent_patten
      partition (dt = '%(ld_daybegin)s')
        select '%(ld_daybegin)s' fdate,
               a.fwinner_bpid,
               a.fwinner_uid,
               a.floser_uid,
               b.fsupport_cnt,
               b.fwin_rate,
               a.fwin_confidence,
               a.flose_confidence,
               case
                  when flose_confidence >= %(min_confidence)s and fwin_confidence >= %(min_confidence)s then 3
                  when flose_confidence >= %(min_confidence)s then 1
                  when fwin_confidence >= %(min_confidence)s then 2 end ftype
          from stage.user_gameparty_confidence_two_%(num_begin)s a
         inner join stage.user_gameparty_support_two_%(num_begin)s b
            on a.fgamefsk = b.fgamefsk
           and a.fplatformfsk = b.fplatformfsk
           and a.fwinner_uid = b.fwinner_uid
           and a.floser_uid = b.floser_uid
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    hql = """
      drop table if exists stage.user_gameparty_support_one_%(num_begin)s;
      drop table if exists stage.user_gameparty_support_two_%(num_begin)s;
      drop table if exists stage.user_gameparty_confidence_two_%(num_begin)s;
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    return res

if __name__ == '__main__':
  #愉快的统计要开始啦
  statDate = get_stat_date()
  #生成统计实例
  a = agg_user_gameparty_frequent_patten(statDate)
  a()
