#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_key_account_spread_data(BaseStat):
  """大客户数量、订单、在线时长、在线人数分布统计
  """
  def create_tab(self):
    hql = """create external table if not exists analysis.user_key_account_spread_fct
            (
              fdate         date,
              fgamefsk      bigint,
              fplatformfsk  bigint,
              fversionfsk   bigint,
              ftype         varchar(64),
              frank         bigint,
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
    insert overwrite table analysis.user_key_account_spread_fct
    partition(dt='%(ld_daybegin)s')
       select fdate,
             fgamefsk,
             fplatformfsk,
             fversionfsk,
             'kaspread' ftype,
             frank,
             count(fuid) fcnt
        from (select fdate,
                     fbpid,
                     fuid,
                     (case
                       when fpayment <= 200 then 1
                       when fpayment > 200 and fpayment <= 300 then 2
                       when fpayment > 300 and fpayment <= 500 then 3
                       when fpayment > 500 and fpayment <= 700 then 4
                       when fpayment > 700 and fpayment <= 1000 then 5
                       when fpayment > 1000 and fpayment <= 2000 then 6
                       when fpayment > 2000 then 7
                     end) frank
                from stage.user_key_account_mid
               where dt = '%(ld_daybegin)s') a
        join analysis.bpid_platform_game_ver_map b
          on a.fbpid = b.fbpid
       group by fdate, fgamefsk, fplatformfsk, fversionfsk, frank

       union all

      select fdate,
             fgamefsk,
             fplatformfsk,
             fversionfsk,
             'kaorder' ftype,
             frank,
             count(fuid) fcnt
        from (select a.fdate,
                     fgamefsk,
                     fplatformfsk,
                     fversionfsk,
                     a.fuid,
                     case
                       when ceil(fusd / 0.157176) = 2 then 1
                       when ceil(fusd / 0.157176) = 5 then 2
                       when ceil(fusd / 0.157176) = 6 then 3
                       when ceil(fusd / 0.157176) = 12 then 4
                       when ceil(fusd / 0.157176) = 25 then 5
                       when ceil(fusd / 0.157176) = 30 then 6
                       when ceil(fusd / 0.157176) = 50 then 7
                       when ceil(fusd / 0.157176) = 100 then 8
                     end frank
                from stage.user_key_account_mid a
                join stage.payment_stream_mid b
                  on a.fbpid = b.fbpid
                 and a.fuid = b.fuid
                 and b.dt = '%(ld_daybegin)s'
                 and b.fdate >= '%(ld_daybegin)s'
                 and b.fdate < '%(ld_dayend)s'
                 and pstatus = 2
                join analysis.bpid_platform_game_ver_map c
                  on a.fbpid = c.fbpid
               where a.dt = '%(ld_daybegin)s') tmp
       group by fdate, fgamefsk, fplatformfsk, fversionfsk, frank

     union all

     select fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            'kadura' ftype,
            frank,
            count(fuid) fcnt
       from (select a.fdate,
                    fgamefsk,
                    fplatformfsk,
                    fversionfsk,
                    a.fuid,
                    case
                      when sum(fplaytime) <= 600 then 1
                      when sum(fplaytime) > 600 and sum(fplaytime) <= 1200 then 2
                      when sum(fplaytime) > 1200 and sum(fplaytime) <= 1800 then 3
                      when sum(fplaytime) > 1800 and sum(fplaytime) <= 3000 then 4
                      when sum(fplaytime) > 3000 and sum(fplaytime) <= 5400 then 5
                      when sum(fplaytime) > 5400 then 6
                    end frank
               from stage.user_key_account_mid a
               join stage.user_gameparty_info_mid b
                 on a.fbpid = b.fbpid
                and a.fuid = b.fuid
                and b.dt = '%(ld_daybegin)s'
               join analysis.bpid_platform_game_ver_map c
                 on a.fbpid = c.fbpid
              where a.dt = '%(ld_daybegin)s'
              group by a.fdate, a.fuid, fgamefsk, fplatformfsk, fversionfsk) tmp
        group by fdate, fgamefsk, fplatformfsk, fversionfsk, frank

      union all

     select fdate,
            fgamefsk,
            fplatformfsk,
            fversionfsk,
            'kahour' ftype,
            frank,
            count(fuid) fcnt
       from (select a.fdate,
                    fgamefsk,
                    fplatformfsk,
                    fversionfsk,
                    a.fuid,
                    hour(b.fls_at)+1 frank
               from stage.user_key_account_mid a
               join stage.finished_gameparty_uid_mid b
                 on a.fbpid = b.fbpid
                and a.fuid = b.fuid
                and b.dt = '%(ld_daybegin)s'
               join analysis.bpid_platform_game_ver_map c
                 on a.fbpid = c.fbpid
              where a.dt = '%(ld_daybegin)s') tmp
        group by fdate, fgamefsk, fplatformfsk, fversionfsk, frank;
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
  a = agg_key_account_spread_data(statDate)
  a()
