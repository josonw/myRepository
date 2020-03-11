#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_active_all(BaseStat):
  def create_tab(self):
    """
    建表：           用户活跃全量表 ，计算日期从2014-01-01开始
    ffirst_act_date: 首次活跃日期
    flast_act_date： 最后活跃日期
    fact_day：       活跃天数
    flogin_num：     登录次数
    fgame_party_num：玩牌局数
    """
    hql = """
      create table if not exists stage.user_active_all
      (
        fdate           date,
        fbpid           varchar(64),
        fuid            int,
        ffirst_act_date date,
        flast_act_date  date,
        fact_day        bigint,
        flogin_num      bigint,
        fgame_party_num bigint
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
    query = {}
    query.update(dates_dict)

    res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
    if res != 0: return res

    # 历史数据与前一天的全量数据汇总
    # 2014-08以前缺少login_num和gameparty_num字段
    hql = """
      insert overwrite table stage.user_active_all
      partition(dt='%(ld_daybegin)s')
      select '%(ld_daybegin)s'      fdate,
             fbpid,
             fuid,
             min(ffirst_act_date)   ffirst_act_date,
             max(flast_act_date)    flast_act_date ,
             sum(fact_day)          fact_day,
             sum(flogin_num)        flogin_num,
             sum(fgame_party_num)   fgame_party_num
      from(
            select fbpid,
                   fuid,
                   ffirst_act_date,
                   flast_act_date,
                   fact_day,
                   flogin_num,
                   fgame_party_num
              from user_active_all
             where dt = '%(ld_1dayago)s'
             union all
             select fbpid,
                    fuid,
                    fdate ffirst_act_date,
                    fdate flast_act_date,
                    1 fact_day,
                    fnew_login_num flogin_num,
                    fnew_login_num fgame_party_num
              from stage.active_user_mid
             where dt = '%(ld_daybegin)s'
      ) t
       group by fbpid, fuid
    """ % query

    res = self.hq.exe_sql(hql)
    if res != 0: return res

    return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_active_all(statDate)
    a()
