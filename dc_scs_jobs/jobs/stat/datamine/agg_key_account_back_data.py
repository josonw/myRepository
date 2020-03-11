#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_key_account_back_data(BaseStat):
  """建立大客户流失召回结果表
  """
  def create_tab(self):
    hql = """create external table if not exists analysis.user_key_account_back_fct
            (
              fdate         date,
              fgamefsk      bigint,
              fplatformfsk  bigint,
              fversionfsk   bigint,
              ftype         varchar(32),
              fnum          bigint
            )
            partitioned by (dt date)"""
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    return res

  def stat(self):
    # self.hq.debug = 0
    dates_dict = PublicFunc.date_define(self.stat_date)
    ld_9dayago = (datetime.datetime.strptime(self.stat_date, '%Y-%m-%d') - datetime.timedelta(days=9)).strftime('%Y-%m-%d')
    query = {"ld_9dayago": ld_9dayago}
    query.update(dates_dict)
    # 注意开启动态分区
    res = self.hq.exe_sql("""use stage;
      set hive.exec.dynamic.partition.mode=nonstrict;""")
    if res != 0: return res

    hql = """drop table if exists analysis.user_key_account_back_tmp_00_%(num_begin)s;
             create table analysis.user_key_account_back_tmp_00_%(num_begin)s as
             select b.ftime fdate, fbpid, fuid, a.ftype, a.fid
               from analysis.user_lost_keyclient_info a
               join analysis.user_key_account_back_mid b
                 on a.fid = b.fid
                and a.ftype = b.ftype
                and b.ftime = '%(ld_8dayago)s'
                and b.fstatus = '成功'
              where dt = '%(ld_9dayago)s'"""% query
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    # 短信发送成功数
    hql = """drop table if exists analysis.user_key_account_back_tmp_01_%(num_begin)s;
             create table analysis.user_key_account_back_tmp_01_%(num_begin)s as
             select b.ftime fdate,
                     fgamefsk,
                     fplatformfsk,
                     fversionfsk,
                     'kasent' ftype,
                     count(distinct a.fuid) fnum
                from analysis.user_lost_keyclient_info a
                join analysis.user_key_account_back_mid b
                  on a.fid = b.fid
                 and a.ftype = b.ftype
                 and b.ftime = '%(ld_daybegin)s'
                 and b.fstatus = '成功'
                join analysis.bpid_platform_game_ver_map c
                  on a.fbpid = c.fbpid
               where a.dt = '%(ld_1dayago)s'
               group by b.ftime, fgamefsk, fplatformfsk, fversionfsk"""% query
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    # 短信发送成功后七天有活跃的回流数
    hql = """drop table if exists analysis.user_key_account_back_tmp_02_%(num_begin)s;
             create table analysis.user_key_account_back_tmp_02_%(num_begin)s as
             select  a.fdate,
                     fgamefsk,
                     fplatformfsk,
                     fversionfsk,
                     'kaback' ftype,
                     count(distinct a.fuid) fnum
                from analysis.user_key_account_back_tmp_00_%(num_begin)s a
                join analysis.bpid_platform_game_ver_map b
                  on a.fbpid = b.fbpid
                left semi join stage.active_user_mid c
                  on c.dt >= '%(ld_7dayago)s'
                 and c.dt < '%(ld_dayend)s'
                 and a.fbpid = c.fbpid 
                 and a.fuid = c.fuid
               group by a.fdate, fgamefsk, fplatformfsk, fversionfsk"""% query
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    # 短信发送成功后七天有活跃的当天付费用户数
    hql = """drop table if exists analysis.user_key_account_back_tmp_03_%(num_begin)s;
             create table analysis.user_key_account_back_tmp_03_%(num_begin)s as
             select t1.fdate,
                     fgamefsk,
                     fplatformfsk,
                     fversionfsk,
                     'kabackpay' ftype,
                     count(distinct t1.fuid) fnum
                from
                (
                  select * 
                  from analysis.user_key_account_back_tmp_00_%(num_begin)s a
                  left semi join stage.active_user_mid b
                    on b.dt >= '%(ld_7dayago)s'
                   and b.dt < '%(ld_dayend)s'
                   and a.fbpid = b.fbpid 
                   and a.fuid = b.fuid
                ) t1
                join stage.pay_user_mid d
                  on t1.fbpid = d.fbpid
                 and t1.fuid = d.fuid
                join stage.payment_stream_mid e
                  on e.dt = '%(ld_daybegin)s'
                 and d.fplatform_uid = e.fplatform_uid
                 and d.fbpid = e.fbpid
                join analysis.bpid_platform_game_ver_map f
                  on t1.fbpid = f.fbpid
               group by t1.fdate, fgamefsk, fplatformfsk, fversionfsk"""% query
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    # 汇总
    hql = """ insert overwrite table analysis.user_key_account_back_fct
              partition(dt='%(ld_daybegin)s')
              select fdate, fgamefsk, fplatformfsk, fversionfsk, ftype, fnum
                from analysis.user_key_account_back_tmp_01_%(num_begin)s              
              union all
              select fdate, fgamefsk, fplatformfsk, fversionfsk, ftype, fnum
                from analysis.user_key_account_back_tmp_02_%(num_begin)s
              union all
              select fdate, fgamefsk, fplatformfsk, fversionfsk, ftype, fnum
                from analysis.user_key_account_back_tmp_03_%(num_begin)s; """ % query
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    # 干掉临时工
    hql = """ drop table if exists analysis.user_key_account_back_tmp_00_%(num_begin)s;               
              drop table if exists analysis.user_key_account_back_tmp_01_%(num_begin)s;
              drop table if exists analysis.user_key_account_back_tmp_02_%(num_begin)s;
              drop table if exists analysis.user_key_account_back_tmp_03_%(num_begin)s;
          """% query
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
  a = agg_key_account_back_data(statDate)
  a()
