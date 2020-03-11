#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_key_account_trend_data(BaseStat):
  """建立游戏维度表
  """
  def create_tab(self):
    hql = """create external table if not exists analysis.user_key_account_trend_fct
            (
            fdate         date,
            fgamefsk      bigint,
            fplatformfsk  bigint,
            fversionfsk   bigint,
            ftype         varchar(64),
            fnum          decimal(20,2)
            )
            partitioned by (dt date)"""
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    return res

  def stat(self):
    # self.hq.debug = 0
    dates_dict = PublicFunc.date_define( self.stat_date )
    query = {}
    query.update( dates_dict )
    # 注意开启动态分区
    res = self.hq.exe_sql("""use stage;
        set hive.exec.dynamic.partition.mode=nonstrict;""")
    if res != 0: return res

    hql = """
            insert overwrite table analysis.user_key_account_trend_fct
            partition(dt='%(ld_daybegin)s')
          select fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 'kacnt',
                 count(fuid) fnum
            from stage.user_key_account_mid a
            join analysis.bpid_platform_game_ver_map b
              on a.fbpid = b.fbpid
           where a.dt = '%(ld_daybegin)s'
           group by fdate, fgamefsk, fplatformfsk, fversionfsk

           union all

          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 'kadau',
                 count(a.fuid) fnum
            from (select fbpid, fuid
                    from stage.user_key_account_mid
                   where dt = '%(ld_daybegin)s') a
          left semi join (select fbpid, fuid
                    from stage.active_user_mid
                   where dt = '%(ld_daybegin)s') b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
            join analysis.bpid_platform_game_ver_map c
              on a.fbpid = c.fbpid
           group by fgamefsk, fplatformfsk, fversionfsk

           union all

          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 'ka7dau',
                 count(a.fuid) fnum
            from (select fbpid, fuid
                   from stage.user_key_account_mid
                  where dt = '%(ld_daybegin)s') a
            left semi join (select fbpid, fuid
                   from stage.active_user_mid
                  where dt >= '%(ld_6dayago)s'
                    and dt < '%(ld_dayend)s') b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
           join analysis.bpid_platform_game_ver_map c
             on a.fbpid = c.fbpid
          group by fgamefsk, fplatformfsk, fversionfsk

           union all

          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 'ka30dau',
                 count(a.fuid) fnum
            from (select fbpid, fuid
                    from stage.user_key_account_mid
                   where dt = '%(ld_daybegin)s') a
            left semi join (select fbpid, fuid
                    from stage.active_user_mid
                   where dt >= '%(ld_29dayago)s'
                     and dt < '%(ld_dayend)s') b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
            join analysis.bpid_platform_game_ver_map c
              on a.fbpid = c.fbpid
           group by fgamefsk, fplatformfsk, fversionfsk

          union all

          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 'kadpu',
                 count(a.fuid) fnum
            from (select fbpid, fuid
                    from stage.user_key_account_mid
                   where dt = '%(ld_daybegin)s') a
          left semi join (select fbpid, fuid
                    from stage.payment_stream_mid
                   where dt = '%(ld_daybegin)s') b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
            join analysis.bpid_platform_game_ver_map c
              on a.fbpid = c.fbpid
           group by fgamefsk, fplatformfsk, fversionfsk

            union all

          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 'kadip',
                 sum(fusd) / 0.157176 fnum
            from stage.user_key_account_mid a
            join stage.payment_stream_mid b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
             and b.dt = '%(ld_daybegin)s'
             and b.fdate >= '%(ld_daybegin)s'
             and b.fdate < '%(ld_dayend)s'
            join analysis.bpid_platform_game_ver_map c
              on a.fbpid = c.fbpid
           where a.dt = '%(ld_daybegin)s'
           group by fgamefsk, fplatformfsk, fversionfsk

          union all

          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 'kabankrupt',
                 count(a.fuid) fnum
            from (select fbpid, fuid
                    from stage.user_key_account_mid
                   where dt = '%(ld_daybegin)s') a
          left semi join (select fbpid, fuid
                            from stage.user_bankrupt_stg
                           where dt = '%(ld_daybegin)s') b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
            join analysis.bpid_platform_game_ver_map c
              on a.fbpid = c.fbpid
           group by fgamefsk, fplatformfsk, fversionfsk

           union all

          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 'kapun',
                 count(a.fuid) fnum
            from (select fbpid, fuid
                    from stage.user_key_account_mid
                   where dt = '%(ld_daybegin)s') a
          left semi join (select fbpid, fuid
                            from stage.user_gameparty_info_mid
                           where dt = '%(ld_daybegin)s') b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
            join analysis.bpid_platform_game_ver_map c
              on a.fbpid = c.fbpid
           group by fgamefsk, fplatformfsk, fversionfsk

          union all

          select '%(ld_daybegin)s' fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 'kapn',
                 sum(fparty_num) fnum
            from stage.user_key_account_mid a
            join stage.user_gameparty_info_mid b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
             and b.dt = '%(ld_daybegin)s'
            join analysis.bpid_platform_game_ver_map c
              on a.fbpid = c.fbpid
           where a.dt = '%(ld_daybegin)s'
           group by fgamefsk, fplatformfsk, fversionfsk;

    """ % query
    res = self.hq.exe_sql(hql)
    if res != 0: return res
    return res

if __name__ == '__main__':
  #愉快的统计要开始啦
  global statDate
  if len(sys.argv) == 1:
      #没有输入参数的话，日期默认取昨天
      statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
  else:
      #从输入参数取，实际是第几个就取第几个
      args = sys.argv[1].split(',')
      statDate = args[0]
  #生成统计实例
  a = agg_key_account_trend_data(statDate)
  a()
