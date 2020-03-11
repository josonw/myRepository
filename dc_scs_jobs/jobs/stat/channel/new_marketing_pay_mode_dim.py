#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class new_marketing_pay_mode_dim(BaseStat):


    def create_tab(self):
        hql = """
        create table if not exists analysis.marketing_pay_mode_money
        (
          fdate       date,
          fchannel_id varchar(64),
          fpay_mode   varchar(256),
          fpay_money  decimal(20,2),
          fpay_rate   decimal(20,7)
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        hql_list = []
        hql = """

        insert overwrite table analysis.marketing_pay_mode_money partition
          (dt = "%(ld_begin)s")
           select fdate,
                 fchannel_id,
                 fpay_mode,
                 round( sum( fpay_money * fpay_rate/coalesce(rate.rate, 0.157176) ),  2) fpay_money,
                 fpay_rate
            from stage.channel_market_payment_mid a
            LEFT JOIN (
                select * from stage.paycenter_rate rate
                where
                    rate.dt='%(ld_begin)s' AND rate.unit='CNY'
                limit 1
            ) rate
                ON rate.dt='%(ld_begin)s' AND rate.unit='CNY'
           where a.dt = '%(ld_begin)s'
           group by fdate, fchannel_id, fpay_mode, fpay_rate;


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
    a = new_marketing_pay_mode_dim(stat_date)
    a()