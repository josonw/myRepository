#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_key_account_spread_ctgy_data(BaseStat):
  """大客户迭代版本、来源渠道、消耗分布、购买分布统计
  """
  def create_tab(self):
    hql = """create external table if not exists analysis.user_key_account_spread_ctgy_fct
            (
            fdate         date,
            fgamefsk      bigint,
            fplatformfsk  bigint,
            fversionfsk   bigint,
            ftype         varchar(64),
            fcategory     varchar(64),
            fnum          bigint
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
    insert overwrite table analysis.user_key_account_spread_ctgy_fct
    partition(dt='%(ld_daybegin)s')      
      select fdate,
             fgamefsk,
             fplatformfsk,
             fversionfsk,
             'kaversion' ftype,
             coalesce(fversion_info,'其他') fcategory,
             count(distinct a.fuid) fnum
        from stage.user_key_account_mid a
        join (select fbpid, fuid, max(fversion_info) fversion_info
                from stage.user_login_stg
               where dt = '%(ld_daybegin)s'
               group by fbpid, fuid) b
          on a.fbpid = b.fbpid
         and a.fuid = b.fuid
        join analysis.bpid_platform_game_ver_map c
          on a.fbpid = c.fbpid
       where a.dt = '%(ld_daybegin)s'
       group by fdate, fgamefsk, fplatformfsk, fversionfsk, fversion_info

      union all

      select fdate,
             fgamefsk,
             fplatformfsk,
             fversionfsk,
             'karegchnl' ftype,
             coalesce(d.name, '其他') fcategory,
             count(distinct a.fuid) fnum
        from stage.user_key_account_mid a
        left outer join stage.channel_market_reg_mid b
          on a.fbpid = b.fbpid
         and a.fuid = b.fuid
        join analysis.dc_channel_package c
          on b.fnow_channel_id = c.fpkg_id
        join analysis.dc_channel d
          on c.fpkg_channel_id = d.fchannel_id
        join analysis.bpid_platform_game_ver_map e
          on a.fbpid = e.fbpid
       where a.dt = '%(ld_daybegin)s'
       group by fdate, fgamefsk, fplatformfsk, fversionfsk, d.name

      union all

      select a.fdate,
             fgamefsk,
             fplatformfsk,
             fversionfsk,
             'kapaychnl' ftype,
             coalesce(c.fm_name, '其他') fcategory,
             count(distinct a.fuid) fnum
        from stage.user_key_account_mid a
        join stage.pay_user_mid b
          on a.fbpid = b.fbpid
         and a.fuid = b.fuid
        join stage.payment_stream_stg c
          on b.fbpid = c.fbpid
         and b.fplatform_uid = c.fplatform_uid
         and c.dt = '%(ld_daybegin)s'
        join analysis.bpid_platform_game_ver_map d
          on a.fbpid = d.fbpid
       where a.dt = '%(ld_daybegin)s'
       group by a.fdate, fgamefsk, fplatformfsk, fversionfsk, c.fm_name

       union all

       select a.fdate,
              fgamefsk,
              fplatformfsk,
              fversionfsk,
              'kaparty' ftype,
              coalesce(b.fname,'其他') fcategory,
              count(distinct a.fuid) fnum
         from stage.user_key_account_mid a
         join stage.finished_gameparty_uid_mid b
           on a.fbpid = b.fbpid
          and a.fuid = b.fuid
          and b.dt = '%(ld_daybegin)s'
         join analysis.bpid_platform_game_ver_map c
           on a.fbpid = c.fbpid
        where a.dt = '%(ld_daybegin)s'
        group by a.fdate, fgamefsk, fplatformfsk, fversionfsk, b.fname
       
       union all

       select a.fdate,
              fgamefsk,
              fplatformfsk,
              fversionfsk,
              'kapayprod' ftype,
              coalesce(d.fname,'其他') fcategory,
              count(distinct a.fuid) fnum
         from stage.user_key_account_mid a
         join stage.pay_user_mid b
           on a.fbpid = b.fbpid
          and a.fuid = b.fuid
         join stage.payment_stream_stg c
           on b.fbpid = c.fbpid
          and b.fplatform_uid = c.fplatform_uid
          and c.dt = '%(ld_daybegin)s'
         join analysis.payment_product_dim d
           on c.fp_id = d.fp_id 
          and c.fp_name = d.fp_name
         join analysis.bpid_platform_game_ver_map e
           on a.fbpid = e.fbpid
        where a.dt = '%(ld_daybegin)s'
        group by a.fdate, fgamefsk, fplatformfsk, fversionfsk, d.fname

        union all

       select a.fdate,
              c.fgamefsk,
              fplatformfsk,
              fversionfsk,
              'kaprops' ftype,
              coalesce(d.fname,'其他') fcategory,
              sum(b.act_num) fnum
         from stage.user_key_account_mid a
         join stage.pb_props_stream_stg b
           on a.fbpid = b.fbpid
          and a.fuid = b.fuid
          and b.dt = '%(ld_daybegin)s'
          and act_type = 2
         join analysis.bpid_platform_game_ver_map c
           on a.fbpid = c.fbpid
         join analysis.game_coin_type_dim d
           on b.prop_id = d.ftype
          and c.fgamefsk = d.fgamefsk
          and d.fdirection = 'OUT'
        where a.dt = '%(ld_daybegin)s'
        group by a.fdate, c.fgamefsk, fplatformfsk, fversionfsk, d.fname;
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
  a = agg_key_account_spread_ctgy_data(statDate)
  a()
