#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

# 特殊不能多日期并行执行
class new_marketing_ROI_retain_pay(BaseStat):

    def create_tab(self):
        hql = """
        -- 特殊同步要求, 全表
        create table if not exists analysis.marketing_roi_retain_pay
        (
          fsignup_month date,
          fchannel_id   varchar(64),
          fdru_month    bigint,
          fdip          decimal(20,2),
          fdpu_d        bigint
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

        drop table if exists analysis.marketing_roi_retain_pay_tmp_%(num_begin)s;

        create table if not exists analysis.marketing_roi_retain_pay_tmp_%(num_begin)s as
        select trunc(b.dt, 'MM') fsignup_month,
               a.fchannel_id,
               months_between('%(ld_month_begin)s', trunc(b.dt, 'MM')) fdru_month,
               round(sum(fpay_money * fpay_rate / 0.157176), 2) fdip,
               count(distinct a.fudid) fdpu_d
          from stage.channel_market_payment_mid a
          join stage.channel_market_new_reg_mid b
            on a.fbpid = b.fbpid
           and a.fudid = b.fudid
         where a.dt >= '%(ld_month_begin)s'
           and a.dt < '%(ld_end)s'
           and months_between('%(ld_month_begin)s', trunc(b.dt, 'MM')) >= 0
         group by trunc(b.dt, 'MM'),
                  a.fchannel_id,
                  months_between('%(ld_month_begin)s', trunc(b.dt, 'MM'));

        """ % self.hql_dict
        hql_list.append( hql )

        # 清空当前要更新的值
        hql = """
        insert overwrite table analysis.marketing_roi_retain_pay
        select  * from analysis.marketing_roi_retain_pay
         where  fsignup_month != '%(ld_month_begin)s'
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.marketing_roi_retain_pay
        select  coalesce(cast(b.fsignup_month as string), cast(a.fsignup_month as string)),
                coalesce(b.fchannel_id, a.fchannel_id),
                coalesce(b.fdru_month, a.fdru_month),
                coalesce(b.fdip, a.fdip),
                coalesce(b.fdpu_d, a.fdpu_d)
          from analysis.marketing_roi_retain_pay a
          full outer join analysis.marketing_roi_retain_pay_tmp_%(num_begin)s b
            on a.fsignup_month = b.fsignup_month
           and a.fchannel_id = b.fchannel_id
           and a.fdru_month = b.fdru_month
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists analysis.marketing_roi_retain_pay_tmp_%(num_begin)s;
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
    a = new_marketing_ROI_retain_pay(stat_date)
    a()
