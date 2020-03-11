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

class agg_user_gameparty_cheating_uid_fct(BaseStat):
  def create_tab(self):
    """
    根据用户的金币变动和输赢关系的频繁模式，找出疑似作弊的用户id
    fuid：                   疑似作弊用户id
    ffound_type：            识别的类型，是基于金币，还是基于输赢局数
    ftype_cn：               作弊类型，中文标识，囤币、倒币还是合作刷分
    ftype_int：              作弊类型，数字标识
    fwin_inning_all：        当天赢得总牌局数
    flose_inning_all：       当天输掉总牌局数
    fwin_gamecoins_all：     当天赢得总金币数
    flose_gamecoins_all：    当天输掉总金币数
    fwin_rate：              胜率
    fwin_confidence：        赢牌置信度
    flose_confidence：       输牌置信度
    fwin_inning_pair：       在对弈中赢得最多一次的牌局数
    flose_inning_pair：      在对弈中输得最多一次的牌局数
    fwin_gamecoins_pair：    在对弈中赢得最多一次的金币数
    flose_gamecoins_pair：   在对弈中赢得最多一次的金币数
    """
    hql = """
      use analysis;
      create table if not exists analysis.user_gameparty_cheating_uid_fct
      (
            fdate                date,
            fgamefsk             bigint,
            fplatformfsk         bigint,
            fversionfsk          bigint,
            fuid                 int,
            ffound_type          varchar(16),
            ftype_cn             varchar(16),
            ftype_int            tinyint,
            fwin_inning_all      int,
            flose_inning_all     int,
            fwin_gamecoins_all   bigint,
            flose_gamecoins_all  bigint,
            fwin_rate            decimal(30, 6),
            fwin_confidence      decimal(30, 6),
            flose_confidence     decimal(30, 6),
            fwin_inning_pair     int,
            flose_inning_pair    int,
            fwin_gamecoins_pair  bigint,
            flose_gamecoins_pair bigint
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
          insert overwrite table analysis.user_gameparty_cheating_uid_fct partition
          (dt = '%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 t1.fgamefsk,
                 t1.fplatformfsk,
                 t1.fversionfsk,
                 t1.fuid,
                 t1.ffound_type,
                 t1.ftype_cn,
                 t1.ftype_int,
                 sum(case when t2.fgamecoins > 0 then 1 else 0 end) fwin_inning_all,
                 sum(case when t2.fgamecoins < 0 then 1 else 0 end) flose_inning_all,
                 sum(case when t2.fgamecoins > 0 then t2.fgamecoins else 0 end) fwin_gamecoins_all,
                 sum(case when t2.fgamecoins < 0 then -1 * t2.fgamecoins else 0 end) flose_gamecoins_all,
                 t1.fwin_rate,
                 t1.fwin_confidence,
                 t1.flose_confidence,
                 t1.fwin_inning_pair,
                 t1.flose_inning_pair,
                 t1.fwin_gamecoins_pair,
                 t1.flose_gamecoins_pair
            from (select a.fgamefsk,
                         a.fplatformfsk,
                         a.fversionfsk,
                         b.fbpid,
                         a.fuid,
                         a.ffound_type,
                         a.ftype_int,
                         a.ftype_cn,
                         max(fwin_rate) fwin_rate,
                         max(fwin_confidence) fwin_confidence,
                         max(flose_confidence) flose_confidence,
                         max(fwin_inning) fwin_inning_pair,
                         max(flose_inning) flose_inning_pair,
                         max(fwin_gamecoins) fwin_gamecoins_pair,
                         max(flose_gamecoins) flose_gamecoins_pair
                    from (select fgamefsk,
                                 fplatformfsk,
                                 fversionfsk,
                                 fwinner_uid fuid,
                                 ffound_type,
                                 fcheating_type ftype_int,
                                 '囤币大号' ftype_cn,
                                 fwin_rate,
                                 fwin_confidence,
                                 flose_confidence,
                                 fwin_inning,
                                 flose_inning,
                                 fwin_gamecoins,
                                 flose_gamecoins
                            from analysis.user_gameparty_cheating_pair_fct
                           where dt = '%(ld_daybegin)s'
                             and fcheating_type = 1

                          union all

                          select fgamefsk,
                                 fplatformfsk,
                                 fversionfsk,
                                 fwinner_uid fuid,
                                 ffound_type,
                                 fcheating_type ftype_int,
                                 '倒币买家' ftype_cn,
                                 fwin_rate,
                                 fwin_confidence,
                                 flose_confidence,
                                 fwin_inning,
                                 flose_inning,
                                 fwin_gamecoins,
                                 flose_gamecoins
                            from analysis.user_gameparty_cheating_pair_fct
                           where dt = '%(ld_daybegin)s'
                             and fcheating_type = 2
                          union all

                          select fgamefsk,
                                 fplatformfsk,
                                 fversionfsk,
                                 fwinner_uid fuid,
                                 ffound_type,
                                 fcheating_type ftype_int,
                                 '合作刷分' ftype_cn,
                                 fwin_rate,
                                 fwin_confidence,
                                 flose_confidence,
                                 fwin_inning,
                                 flose_inning,
                                 fwin_gamecoins,
                                 flose_gamecoins
                            from analysis.user_gameparty_cheating_pair_fct
                           where dt = '%(ld_daybegin)s'
                             and fcheating_type = 3
                          ) a
                   inner join analysis.bpid_platform_game_ver_map b
                      on a.fgamefsk = b.fgamefsk
                     and a.fplatformfsk = b.fplatformfsk
                     and a.fversionfsk = b.fversionfsk
                   group by a.fgamefsk,
                            a.fplatformfsk,
                            a.fversionfsk,
                            b.fbpid,
                            a.fuid,
                            a.ffound_type,
                            a.ftype_int,
                            a.ftype_cn) t1
           inner join stage.user_gameparty_stg t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t2.dt = '%(ld_daybegin)s'
             and coalesce(t2.fmatch_id, '0') = '0'
           group by t1.fgamefsk,
                    t1.fplatformfsk,
                    t1.fversionfsk,
                    t1.fuid,
                    t1.ffound_type,
                    t1.ftype_int,
                    t1.ftype_cn,
                    t1.fwin_rate,
                    t1.fwin_confidence,
                    t1.flose_confidence,
                    t1.fwin_inning_pair,
                    t1.flose_inning_pair,
                    t1.fwin_gamecoins_pair,
                    t1.flose_gamecoins_pair
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    return res

if __name__ == '__main__':
  #愉快的统计要开始啦
  statDate = get_stat_date()
  #生成统计实例
  a = agg_user_gameparty_cheating_uid_fct(statDate)
  a()