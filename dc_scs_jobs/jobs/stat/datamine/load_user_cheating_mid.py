#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

class load_user_cheating_mid(BaseStat):
  """建立刷分用户中间表
  """
  def create_tab(self):
    hql = """
            create external table if not exists stage.user_cheating_mid
            (
              fdate       date,
              fbpid       varchar(64),
              fuid        bigint,
              fplatform   bigint
            )
            partitioned by (dt date)"""
    result = self.hq.exe_sql(hql)
    if result != 0: return result

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
        insert overwrite table stage.user_cheating_mid
        partition(dt='%(ld_daybegin)s')
          select fdate , fbpid, fuid, a.fplatform
            from stage.user_cheating_orgn a
            join stage.user_cheating_bpid_map b
              on a.fplatform = b.fplatform 
             and a.fdate = '%(ld_daybegin)s';
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
  a = load_user_cheating_mid(statDate)
  a()
