#-*- coding: UTF-8 -*-
# Author:AnsenWen
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_login_all(BaseStat):
  def create_tab(self):
    ## 建表，用户分月登录信息汇总
    hql = """
      create table if not exists stage.user_login_all
      (
        fdate           date,
        fbpid           varchar(64),
        fuid            int,
        ffirst_date     date,
        flast_date      date,
        flogin_num      bigint,
        flogin_day      int,
        fm_dtype        varchar(64),
        fm_pixel        varchar(64),
        fm_imei         varchar(64),
        fip_country     varchar(64),
        fip_province    varchar(64),
        fip_city        varchar(64)
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
      drop table if exists stage.user_login_all_tmp_00_%(num_begin)s;
      create table stage.user_login_all_tmp_00_%(num_begin)s as
      select fbpid,
             fuid,
             ffirst_date,
             flast_date,
             flogin_num,
             flogin_day,
             fm_dtype,
             fm_pixel,
             fm_imei,
             fip_country,
             fip_province,
             fip_city
       from stage.user_login_all
      where dt = '%(ld_1dayago)s'

      union all

      select fbpid,
             fuid,
             ffirst_date,
             fdate flast_date,
             flogin_num,
             1 flogin_day,
             fm_dtype,
             fm_pixel,
             fm_imei,
             fip_country,
             fip_province,
             fip_city
       from stage.user_login_mid
      where dt = '%(ld_daybegin)s'
    """ % query
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    hql = """
      drop table if exists stage.user_login_all_tmp_01_%(num_begin)s;
      create table stage.user_login_all_tmp_01_%(num_begin)s as
      select fbpid,
             fuid,
             ffirst_date,
             flast_date,
             null flogin_num,
             null flogin_day,
             fm_dtype,
             fm_pixel,
             fm_imei,
             fip_country,
             fip_province,
             fip_city
      from(
              select fbpid,
                     fuid,
                     ffirst_date,
                     flast_date,
                     fm_dtype,
                     fm_pixel,
                     fm_imei,
                     fip_country,
                     fip_province,
                     fip_city,
                     row_number() over(partition by fbpid, fuid order by flast_date desc) nrow
                from stage.user_login_all_tmp_00_%(num_begin)s
          )t
      where nrow = 1
    """ % query
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    hql = """
      drop table if exists stage.user_login_all_tmp_02_%(num_begin)s;
      create table stage.user_login_all_tmp_02_%(num_begin)s as
      select fbpid,
             fuid,
             min(ffirst_date) ffirst_date,
             max(flast_date) flast_date,
             sum(flogin_num) flogin_num,
             sum(flogin_day) flogin_day,
             null fm_dtype,
             null fm_pixel,
             null fm_imei,
             null fip_country,
             null fip_province,
             null fip_city
        from stage.user_login_all_tmp_00_%(num_begin)s
       group by fbpid, fuid
    """ % query
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    hql = """
      insert overwrite table stage.user_login_all
      partition(dt='%(ld_daybegin)s')
      select '%(ld_daybegin)s' fdate,
             fbpid,
             fuid,
             min(ffirst_date) ffirst_date,
             max(flast_date) flast_date,
             sum(flogin_num) flogin_num,
             sum(flogin_day) flogin_day,
             max(fm_dtype) fm_dtype,
             max(fm_pixel) fm_pixel,
             max(fm_imei) fm_imei,
             max(fip_country) fip_country,
             max(fip_province) fip_province,
             max(fip_city) fip_city
      from(
            select * from stage.user_login_all_tmp_01_%(num_begin)s
            union all
            select * from stage.user_login_all_tmp_02_%(num_begin)s
          )t
      group by fbpid, fuid
    """ % query
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    hql = """
      drop table if exists stage.user_login_all_tmp_00_%(num_begin)s;
      drop table if exists stage.user_login_all_tmp_01_%(num_begin)s;
      drop table if exists stage.user_login_all_tmp_02_%(num_begin)s;
    """ % query
    res = self.hq.exe_sql(hql)
    if res != 0: return res

    return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_login_all(statDate)
    a()
