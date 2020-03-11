#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_gameparty_pairs_mid(BaseStat):
  def create_tab(self):
    """
    建表，玩家两两之间的输赢关系中间表
    fwinner_bpid：   赢较多的玩家bpid
    fwinner_uid：    赢较多的玩家uid
    floser_uid：     输较多（赢较少）的玩家uid
    fwin_inning：    赢较多的玩家赢得牌局数
    flose_inning：   输较多的玩家输掉牌局数
    fwin_gamecoins： 赢较多的玩家赢得金币数
    flose_gamecoins：输较多的玩家输掉金币数
    """
    hql = """
      create table if not exists stage.gameparty_pairs_mid
      (
        fdate           date,
        fgamefsk        bigint,
        fplatformfsk    bigint,
        fis_match       tinyint,
        fwinner_bpid    varchar(100),
        fwinner_uid     int,
        floser_uid      int,
        fwin_inning     int,
        flose_inning    int,
        fwin_gamecoins  bigint,
        flose_gamecoins bigint
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
      drop table if exists stage.gameparty_pairs_tmp_01_%(num_begin)s;
      create table stage.gameparty_pairs_tmp_01_%(num_begin)s as
      select '%(ld_daybegin)s' fdate,
             a.fgamefsk,
             a.fplatformfsk,
             a.fbpid       fwinner_bpid,
             a.fuid        fwinner_uid,
             b.fuid        floser_uid,
             a.ftbl_id,
             a.finning_id,
             a.fpalyer_cnt,
             a.fgamecoins fwin_gamecoins,
             b.fgamecoins flose_gamecoins,
             c.fgamecoins ftotal_gamecoins,
             a.fgamecoins * (-1 * b.fgamecoins) / c.fgamecoins fgamecoins,
             a.fs_timer,
             a.fe_timer,
             a.fsubname,
             a.fpname,
             case when coalesce(a.fmatch_id, '0') = '0' then 0 else 1 end fis_match
        from (select t1.*, t2.fgamefsk, t2.fplatformfsk
                from stage.user_gameparty_stg t1
                join analysis.bpid_platform_game_ver_map t2
                  on t1.fbpid = t2.fbpid
               where t1.dt = '%(ld_daybegin)s'
                 and t1.fgamecoins > 0) a
       inner join (select t1.*, t2.fgamefsk, t2.fplatformfsk
                     from stage.user_gameparty_stg t1
                     join analysis.bpid_platform_game_ver_map t2
                       on t1.fbpid = t2.fbpid
                    where t1.dt = '%(ld_daybegin)s'
                      and t1.fgamecoins < 0) b
          on a.fgamefsk = b.fgamefsk
         and a.fplatformfsk = b.fplatformfsk
         and a.ftbl_id = b.ftbl_id
         and a.finning_id = b.finning_id
       inner join stage.gameparty_inning_mid c
          on a.fgamefsk = c.fgamefsk
         and a.fplatformfsk = c.fplatformfsk
         and a.ftbl_id = c.ftbl_id
         and a.finning_id = c.finning_id
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    hql = """
      drop table if exists stage.gameparty_pairs_tmp_02_%(num_begin)s;
      create table stage.gameparty_pairs_tmp_02_%(num_begin)s as
      select '%(ld_daybegin)s' fdate,
             fgamefsk,
             fplatformfsk,
             fis_match,
             fwinner_bpid,
             fwinner_uid,
             floser_uid,
             count(1) finning_cnt,
             sum(fgamecoins) fgamecoins
        from stage.gameparty_pairs_tmp_01_%(num_begin)s a
       group by fgamefsk,
                fplatformfsk,
                fwinner_bpid,
                fwinner_uid,
                floser_uid,
                fis_match
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    hql = """
      insert overwrite table stage.gameparty_pairs_mid
      partition(dt='%(ld_daybegin)s')
      select '%(ld_daybegin)s' fdate,
             a.fgamefsk,
             a.fplatformfsk,
             a.fis_match,
             a.fwinner_bpid,
             a.fwinner_uid,
             a.floser_uid,
             a.finning_cnt                                              fwin_inning,
             coalesce(b.finning_cnt, 0)                                 flose_inning,
             a.fgamecoins                                               fwin_gamecoins,
             coalesce(b.fgamecoins, 0)                                  flose_gamecoins
        from (select *
                from stage.gameparty_pairs_tmp_02_%(num_begin)s) a
        left outer join (select *
                           from stage.gameparty_pairs_tmp_02_%(num_begin)s) b
          on a.fgamefsk = b.fgamefsk
         and a.fplatformfsk = b.fplatformfsk
         and a.fwinner_uid = b.floser_uid
         and a.floser_uid = b.fwinner_uid
        where a.finning_cnt - coalesce(b.finning_cnt, 0) >= 0
        group by a.fgamefsk,
                 a.fplatformfsk,
                 a.fis_match,
                 a.fwinner_bpid,
                 a.fwinner_uid,
                 a.floser_uid,
                 a.finning_cnt,
                 b.finning_cnt,
                 a.fgamecoins,
                 b.fgamecoins
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    hql = """
      drop table if exists stage.gameparty_pairs_tmp_01_%(num_begin)s;
      drop table if exists stage.gameparty_pairs_tmp_02_%(num_begin)s;
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    return res

if __name__ == '__main__':
  #愉快的统计要开始啦
  statDate = get_stat_date()
  #生成统计实例
  a = agg_gameparty_pairs_mid(statDate)
  a()
