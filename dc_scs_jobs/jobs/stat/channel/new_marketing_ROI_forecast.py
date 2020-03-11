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
class new_marketing_ROI_forecast(BaseStat):
    def create_tab(self):
        hql = """
        create table if not exists analysis.markerting_channel_forecast
        (
          fdate       date,
          fchannel_id varchar(64),
          d0_money    decimal(20,2),
          d1_money    decimal(20,2),
          d1b_money   decimal(20,2),
          d3_money    decimal(20,2),
          d3b_money   decimal(20,2),
          d7_money    decimal(20,2),
          d7b_money   decimal(20,2),
          d15_money   decimal(20,2),
          d15b_money  decimal(20,2),
          d30_money   decimal(20,2),
          d30b_money  decimal(20,2),
          d60_money   decimal(20,2),
          d60b_money  decimal(20,2),
          d90_money   decimal(20,2)
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        hql_list = []
        hql = """
        insert overwrite table analysis.markerting_channel_forecast partition
          (dt = "%(ld_begin)s")
        select '%(ld_begin)s' fdate,a.fchannel_id,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) = 0
                  and a.dt <= date_add('%(ld_begin)s', -1) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d0_money,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) <= 1
                  and a.dt <= date_add('%(ld_begin)s', -1) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d1_money,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) <= 1
                  and a.dt <= date_add('%(ld_begin)s', -3) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d1b_money,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) <= 3
                  and a.dt <= date_add('%(ld_begin)s', -3) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d3_money,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) <= 3
                  and a.dt <= date_add('%(ld_begin)s', -7) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d3b_money,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) <= 7
                  and a.dt <= date_add('%(ld_begin)s', -7) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d7_money,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) <= 7
                  and a.dt <= date_add('%(ld_begin)s', -15) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d7b_money,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) <= 15
                  and a.dt <= date_add('%(ld_begin)s', -15) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d15_money,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) <= 15
                  and a.dt <= date_add('%(ld_begin)s', -30) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d15b_money,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) <= 30
                  and a.dt <= date_add('%(ld_begin)s', -30) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d30_money,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) <= 30
                  and a.dt <= date_add('%(ld_begin)s', -60) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d30b_money,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) <= 60
                  and a.dt <= date_add('%(ld_begin)s', -60) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d60_money,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) <= 60
                  and a.dt <= date_add('%(ld_begin)s', -90) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d60b_money,
            sum( case when datediff( b.dt, a.dt ) >= 0 and datediff( b.dt, a.dt ) <= 90
                  and a.dt <= date_add('%(ld_begin)s', -90) then b.fpay_money * b.fpay_rate / coalesce(rate.rate, 0.157176) end ) d90_money
        from stage.channel_market_new_reg_mid a
        join stage.channel_market_payment_mid b
            on a.fudid = b.fudid
            and a.fbpid = b.fbpid
        LEFT JOIN (
            select * from stage.paycenter_rate rate
            where
                rate.dt='%(ld_begin)s' AND rate.unit='CNY'
            limit 1
        ) rate
            ON rate.dt='%(ld_begin)s' AND rate.unit='CNY'
        where a.dt >= '2014-01-01'
        group by a.fchannel_id
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
    a = new_marketing_ROI_forecast(stat_date)
    a()