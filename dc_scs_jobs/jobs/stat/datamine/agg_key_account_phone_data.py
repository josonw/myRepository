#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_key_account_phone_data(BaseStat):
  """建立我爱斗地主大客户流失用户手机号码信息表
  """
  def create_tab(self):
    hql = """create external table if not exists analysis.user_lost_keyclient_info
            (
              fdate       date,
              fbpid       varchar(64),
              fuid        bigint,
              ftype       varchar(32),
              fid         varchar(32),
              fdate_diff  int
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

    #七天前有活跃，七天后无活跃, 付费大于100rmb
    hql = """
          drop table if exists stage.user_lost_keyclient_tmp_1_%(num_begin)s;
          create table stage.user_lost_keyclient_tmp_1_%(num_begin)s as
          select '%(ld_daybegin)s' as fdate,
                 a.fbpid,
                 a.fuid,
                 ftype,
                 fid,
                 7 as fdate_diff
            from (select t1.fbpid, t1.fuid 
                  from (select fbpid, fuid
                          from stage.user_key_account_mid
                         where dt = '%(ld_daybegin)s') t1
                  left semi join (select fbpid, fuid
                                    from stage.active_user_mid
                                    where dt = '%(ld_7dayago)s') t2
                    on t1.fbpid = t2.fbpid
                   and t1.fuid = t2.fuid
                  left outer join (select fbpid, fuid
                                     from stage.active_user_mid
                                    where dt > '%(ld_7dayago)s'
                                      and dt < '%(ld_dayend)s'
                                      and fuid is not null) t3
                    on t1.fbpid = t3.fbpid
                   and t1.fuid = t3.fuid
                 where t3.fbpid is null
                    or t3.fuid is null) a
            join stage.user_ids_stg b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
             and ftype = 'phone'
           group by a.fuid, a.fbpid, ftype, fid;
    """ % query
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    #14天前有活跃，14天后无活跃, 付费大于100rmb
    hql = """
          drop table if exists stage.user_lost_keyclient_tmp_2_%(num_begin)s;
          create table stage.user_lost_keyclient_tmp_2_%(num_begin)s as
          select '%(ld_daybegin)s' as fdate,
                 a.fbpid,
                 a.fuid,
                 ftype,
                 fid,
                 14 as fdate_diff
            from (select t1.fbpid, t1.fuid 
                  from (select fbpid, fuid
                          from stage.user_key_account_mid
                         where dt = '%(ld_daybegin)s') t1
                  left semi join (select fbpid, fuid
                                    from stage.active_user_mid
                                    where dt = '%(ld_7dayago)s') t2
                    on t1.fbpid = t2.fbpid
                   and t1.fuid = t2.fuid
                  left outer join (select fbpid, fuid
                                     from stage.active_user_mid
                                    where dt > '%(ld_14dayago)s'
                                      and dt < '%(ld_dayend)s'
                                      and fuid is not null) t3
                    on t1.fbpid = t3.fbpid
                   and t1.fuid = t3.fuid
                 where t3.fbpid is null
                    or t3.fuid is null) a
            join stage.user_ids_stg b
              on a.fbpid = b.fbpid
             and a.fuid = b.fuid
             and ftype = 'phone'
           group by a.fuid, a.fbpid, ftype, fid;
    """ % query
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    # union all 两张表
    hql = """
          insert overwrite table analysis.user_lost_keyclient_info
          partition(dt='%(ld_daybegin)s')
          select * from stage.user_lost_keyclient_tmp_1_%(num_begin)s
          union all
          select * from stage.user_lost_keyclient_tmp_2_%(num_begin)s;            
    """ % query
    res = self.hq.exe_sql(hql)
    if res != 0: return res
    
    #干掉临时工
    hql = """
          drop table if exists stage.user_lost_keyclient_tmp_1_%(num_begin)s;
          drop table if exists stage.user_lost_keyclient_tmp_2_%(num_begin)s;            
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
  a = agg_key_account_phone_data(statDate)
  a()
