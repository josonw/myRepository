#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class new_marketing_ROI_start(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.marketing_roi_start_pay_fct
        (
          fsignup_month date,
          fchannel_id   varchar(64),
          fdru_month    int,
          fdip          decimal(20,2),
          fdpu_d        int
        );


        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        # self.hq.debug = 1

        hql_list = []
        hql = """
        add jar hdfs://10.30.101.92:8020/dw/udf/nexr-hive-udf-0.4.jar;
        CREATE TEMPORARY FUNCTION months_between AS 'com.nexr.platform.hive.udf.UDFMonthsBetween';
        CREATE TEMPORARY FUNCTION trunc AS 'com.nexr.platform.hive.udf.UDFTrunc';

        drop table if exists analysis.marketing_roi_start_pay_fct_tmp_%(num_begin)s;

        create table if not exists analysis.marketing_roi_start_pay_fct_tmp_%(num_begin)s as
          select trunc(c.dt, 'MM') fsignup_month,
                 a.fchannel_id,
                 months_between('%(ld_month_begin)s', trunc(c.dt, 'MM')) fdru_month,
                 round( sum( fpay_money * fpay_rate ),  2) fdip,
                 count(distinct a.fudid) fdpu_d
            from stage.channel_market_payment_mid a
            join dim.bpid_map b
              on a.fbpid = b.fbpid
              and b.fgamename != '汇总数据'
            join stage.channel_market_new_start_mid c
              on c.fgamefsk = b.fgamefsk
             and c.fudid = a.fudid
           where a.fdate >= '%(ld_month_begin)s'
             and a.fdate < '%(ld_end)s'
             and months_between('%(ld_month_begin)s', trunc(c.dt, 'MM')) >= 0
           group by trunc(c.dt, 'MM'),
                    a.fchannel_id,
                    months_between('%(ld_month_begin)s', trunc(c.dt, 'MM'))


        """ % self.hql_dict
        hql_list.append( hql )

        # 清空当前要更新的值
        hql = """
        insert overwrite table analysis.marketing_roi_start_pay_fct
        select  * from analysis.marketing_roi_start_pay_fct
         where  fsignup_month != '%(ld_month_begin)s'
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.marketing_roi_start_pay_fct
        select  coalesce(cast(b.fsignup_month as string), cast(a.fsignup_month as string)),
                coalesce(b.fchannel_id, a.fchannel_id),
                coalesce(b.fdru_month, a.fdru_month),
                coalesce(b.fdip, a.fdip),
                coalesce(b.fdpu_d, a.fdpu_d)
          from analysis.marketing_roi_start_pay_fct a
          full outer join analysis.marketing_roi_start_pay_fct_tmp_%(num_begin)s b
            on a.fsignup_month = b.fsignup_month
           and a.fchannel_id = b.fchannel_id
           and a.fdru_month = b.fdru_month
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists analysis.marketing_roi_start_pay_fct_tmp_%(num_begin)s;
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
    a = new_marketing_ROI_start(stat_date)
    a()
