#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

# 已经停止计算
class new_marketing_ROI_frcst_show(BaseStat):

    def create_tab(self):
        hql = """
        -- 特殊同步要求,全表
        create table if not exists analysis.marketing_forecast_show_fct
        (
          fdate       date,
          fchannel_id varchar(64),
          fd0_dip     decimal(20,2),
          fd1_dip     decimal(20,2),
          fd3_dip     decimal(20,2),
          fd7_dip     decimal(20,2),
          fd15_dip    decimal(20,2),
          fd30_dip    decimal(20,2),
          fd60_dip    decimal(20,2),
          fd90_dip    decimal(20,2)
        ) """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        yesterday = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
        self.hql_dict['yesterday'] = yesterday

        hql_list = []
        hql = """
        drop table if exists analysis.marketing_forecast_show_fct_tmp;
         create table analysis.marketing_forecast_show_fct_tmp as
         select a.fdate,
               a.fchannel_id,
                sum(case when a.fdru_day = 0   and datediff('%(yesterday)s', a.fdate) >= 0  then a.fdru_d_num end ) fd0_dip,
                sum(case when a.fdru_day <= 1  and datediff('%(yesterday)s', a.fdate) >= 1  then a.fdru_d_num end ) fd1_dip,
                sum(case when a.fdru_day <= 3  and datediff('%(yesterday)s', a.fdate) >= 3  then a.fdru_d_num end ) fd3_dip,
                sum(case when a.fdru_day <= 7  and datediff('%(yesterday)s', a.fdate) >= 7  then a.fdru_d_num end ) fd7_dip,
                sum(case when a.fdru_day <= 15 and datediff('%(yesterday)s', a.fdate) >= 15 then a.fdru_d_num end ) fd15_dip,
                sum(case when a.fdru_day <= 30 and datediff('%(yesterday)s', a.fdate) >= 30 then a.fdru_d_num end ) fd30_dip,
                sum(case when a.fdru_day <= 60 and datediff('%(yesterday)s', a.fdate) >= 60 then a.fdru_d_num end ) fd60_dip,
                sum(case when a.fdru_day <= 90 and datediff('%(yesterday)s', a.fdate) >= 90 then a.fdru_d_num end ) fd90_dip
          from analysis.user_newchannel_retention a
         where a.fdru_num_type = 2
           and a.dt >= date_add('%(ld_begin)s', - 90)
         group by a.fdate, a.fchannel_id;
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists analysis.marketing_forecast_rate_tmp;
         create table if not exists analysis.marketing_forecast_rate_tmp as
          select fdate,fchannel_id,
                case when d0_money = 0 then 0 else d1_money / d0_money end d1_r,
                case when d1b_money = 0 then 0 else d3_money / d1b_money end d3_r,
                case when d3b_money = 0 then 0 else d7_money / d3b_money end d7_r,
                case when d7b_money = 0 then 0 else d15_money / d7b_money end d15_r,
                case when d15b_money = 0 then 0 else d30_money / d15b_money end d30_r,
                case when d30b_money = 0 then 0 else d60_money / d30b_money end d60_r,
                case when d60b_money = 0 then 0 else d90_money / d60b_money end d90_r
           from analysis.markerting_channel_forecast
           where dt = '%(yesterday)s'
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.marketing_forecast_show_fct_tmp_d0;
         create table analysis.marketing_forecast_show_fct_tmp_d0 as
                select a.fdate,
                 a.fchannel_id,
                 a.fd0_dip,
                 a.fd0_dip * b.d1_r fd1_dip,
                 a.fd0_dip * b.d1_r * b.d3_r fd3_dip,
                 a.fd0_dip * b.d1_r * b.d3_r * b.d7_r fd7_dip,
                 a.fd0_dip * b.d1_r * b.d3_r * b.d7_r * b.d15_r fd15_dip,
                 a.fd0_dip * b.d1_r * b.d3_r * b.d7_r * b.d15_r * b.d30_r fd30_dip,
                 a.fd0_dip * b.d1_r * b.d3_r * b.d7_r * b.d15_r * b.d30_r * b.d60_r fd60_dip,
                 a.fd0_dip * b.d1_r * b.d3_r * b.d7_r * b.d15_r * b.d30_r * b.d60_r * b.d90_r fd90_dip
            from analysis.marketing_forecast_show_fct_tmp a
            left join analysis.marketing_forecast_rate_tmp b
              on a.fchannel_id = b.fchannel_id
           where datediff('%(ld_begin)s', a.fdate) = 0;
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists analysis.marketing_forecast_show_fct_tmp_d1;
         create table analysis.marketing_forecast_show_fct_tmp_d1 as
                   select a.fdate,
                 a.fchannel_id,
                 a.fd0_dip,
                 a.fd1_dip,
                 fd1_dip * d3_r fd3_dip,
                 fd1_dip * d3_r * d7_r fd7_dip,
                 fd1_dip * d3_r * d7_r * d15_r fd15_dip,
                 fd1_dip * d3_r * d7_r * d15_r * d30_r fd30_dip,
                 fd1_dip * d3_r * d7_r * d15_r * d30_r * d60_r fd60_dip,
                 fd1_dip * d3_r * d7_r * d15_r * d30_r * d60_r * d90_r fd90_dip
            from analysis.marketing_forecast_show_fct_tmp a
            left join analysis.marketing_forecast_rate_tmp b
              on a.fchannel_id = b.fchannel_id
           where datediff('%(ld_begin)s', a.fdate) >= 1 and datediff('%(ld_begin)s', a.fdate) < 3;
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.marketing_forecast_show_fct_tmp_d3;
         create table analysis.marketing_forecast_show_fct_tmp_d3 as
                 select a.fdate,
                 a.fchannel_id,
                 a.fd0_dip,
                 a.fd1_dip,
                 a.fd3_dip,
                 fd3_dip * d7_r fd7_dip,
                 fd3_dip * d7_r * d15_r fd15_dip,
                 fd3_dip * d7_r * d15_r * d30_r fd30_dip,
                 fd3_dip * d7_r * d15_r * d30_r * d60_r fd60_dip,
                 fd3_dip * d7_r * d15_r * d30_r * d60_r * d90_r fd90_dip
            from analysis.marketing_forecast_show_fct_tmp a
            left join analysis.marketing_forecast_rate_tmp b
              on a.fchannel_id = b.fchannel_id
           where datediff('%(ld_begin)s', a.fdate) >= 3 and datediff('%(ld_begin)s', a.fdate) < 7;

        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.marketing_forecast_show_fct_tmp_d7;
         create table analysis.marketing_forecast_show_fct_tmp_d7 as
                  select a.fdate,
                 a.fchannel_id,
                 a.fd0_dip,
                 a.fd1_dip,
                 a.fd3_dip,
                 a.fd7_dip,
                 a.fd7_dip * b.d15_r fd15_dip,
                 a.fd7_dip * b.d15_r * b.d30_r fd30_dip,
                 a.fd7_dip * b.d15_r * b.d30_r * b.d60_r fd60_dip,
                 a.fd7_dip * b.d15_r * b.d30_r * b.d60_r * b.d90_r fd90_dip
            from analysis.marketing_forecast_show_fct_tmp a
            left join analysis.marketing_forecast_rate_tmp b
              on a.fchannel_id = b.fchannel_id
           where datediff('%(ld_begin)s', a.fdate) >= 7 and datediff('%(ld_begin)s', a.fdate) < 15;

        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.marketing_forecast_show_fct_tmp_d15;
         create table analysis.marketing_forecast_show_fct_tmp_d15 as
                  select a.fdate,
                 a.fchannel_id,
                 a.fd0_dip,
                 a.fd1_dip,
                 a.fd3_dip,
                 a.fd7_dip,
                 a.fd15_dip,
                 fd15_dip * d30_r fd30_dip,
                 fd15_dip * d30_r * d60_r fd60_dip,
                 fd15_dip * d30_r * d60_r * d90_r fd90_dip
            from analysis.marketing_forecast_show_fct_tmp a
            left join analysis.marketing_forecast_rate_tmp b
              on a.fchannel_id = b.fchannel_id
           where datediff('%(ld_begin)s', a.fdate) >= 15 and datediff('%(ld_begin)s', a.fdate) < 30;
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.marketing_forecast_show_fct_tmp_d30;
         create table analysis.marketing_forecast_show_fct_tmp_d30 as
                 select a.fdate,
                 a.fchannel_id,
                 a.fd0_dip,
                 a.fd1_dip,
                 a.fd3_dip,
                 a.fd7_dip,
                 a.fd15_dip,
                 a.fd30_dip,
                 fd30_dip * d60_r fd60_dip,
                 fd30_dip * d60_r * d90_r fd90_dip
            from analysis.marketing_forecast_show_fct_tmp a
            left join analysis.marketing_forecast_rate_tmp b
              on a.fchannel_id = b.fchannel_id
           where datediff('%(ld_begin)s', a.fdate) >= 30 and datediff('%(ld_begin)s', a.fdate) < 60;
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.marketing_forecast_show_fct_tmp_d60;
         create table analysis.marketing_forecast_show_fct_tmp_d60 as
                 select a.fdate,
                 a.fchannel_id,
                 a.fd0_dip,
                 a.fd1_dip,
                 a.fd3_dip,
                 a.fd7_dip,
                 a.fd15_dip,
                 a.fd30_dip,
                 a.fd60_dip,
                 fd60_dip * d90_r fd90_dip
            from analysis.marketing_forecast_show_fct_tmp a
            left join analysis.marketing_forecast_rate_tmp b
              on a.fchannel_id = b.fchannel_id
           where datediff('%(ld_begin)s', a.fdate) >= 60 and datediff('%(ld_begin)s', a.fdate) < 90;
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        use analysis;
        truncate table marketing_forecast_show_fct;
        insert overwrite table analysis.marketing_forecast_show_fct
        select
            fdate,
            fchannel_id,
            max(fd0_dip) fd0_dip,
            max(fd1_dip) fd1_dip,
            max(fd3_dip) fd3_dip,
            max(fd7_dip) fd7_dip,
            max(fd15_dip) fd15_dip,
            max(fd30_dip) fd30_dip,
            max(fd60_dip) fd60_dip,
            max(fd90_dip) fd90_dip
         from(
            select * from analysis.marketing_forecast_show_fct_tmp a
            union all
            select * from analysis.marketing_forecast_show_fct_tmp_d0 b
            union all
            select * from analysis.marketing_forecast_show_fct_tmp_d1 c
            union all
            select * from analysis.marketing_forecast_show_fct_tmp_d3 d
            union all
            select * from analysis.marketing_forecast_show_fct_tmp_d7 e
            union all
            select * from analysis.marketing_forecast_show_fct_tmp_d15 f
            union all
            select * from analysis.marketing_forecast_show_fct_tmp_d30 g
            union all
            select * from analysis.marketing_forecast_show_fct_tmp_d60 h
        ) t
        group by fdate, fchannel_id

        """ % self.hql_dict
        hql_list.append( hql )

        result = self.exe_hql_list(hql_list)
        return result


if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = new_marketing_ROI_frcst_show(stat_date)
    a()