#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date

class new_marketing_ROI_cost_money_pg(BasePGStat):
    """ 处理用户设置的单价和分成比例
    """

    def stat(self):

        sql = """
           WITH t_select  AS (
         select date_trunc('month', date'%(ld_begin)s')::date fsignup_month, fchannel_id, sum(fdsu_d) fdsu_d
             from (
             select fdate,
                      a.fchannel_id,
                      row_number() over(partition by fdate, a.fchannel_id order by a.fdate - COALESCE(b.fchange_time, a.fdate)) rown,
                      round(fdsu_d * (1 - COALESCE(b.fdiscount, 0)), 0) fdsu_d
                 from analysis.marketing_channel_dims_fct a
                 left join analysis.marketing_channel_pkg_discount b
                   on a.fchannel_id = to_char(b.fpkg_id)
                  and a.fdate > COALESCE(b.fchange_time, a.fdate)
                where fdate >=  date_trunc('month', date'%(ld_begin)s')
                  and fdate < date'%(ld_end)s'
                order by fdate
                ) as abc
            where rown = 1
            and fchannel_id is not null
            group by fchannel_id ),

            updated as ( UPDATE marketing_roi_cost_fct as target SET fdsu_d = t_select.fdsu_d from t_select
                          where target.fsignup_month = t_select.fsignup_month and target.fchannel_id = t_select.fchannel_id
                      RETURNING target.fsignup_month, target.fchannel_id  )

            INSERT INTO marketing_roi_cost_fct(fsignup_month, fchannel_id, fdsu_d)
            SELECT t_select.* FROM t_select
              LEFT JOIN updated on t_select.fsignup_month = updated.fsignup_month
               AND t_select.fchannel_id = updated.fchannel_id
             WHERE updated.fchannel_id IS NULL

        """ % self.sql_dict
        self.append(sql)


        sql = """
            WITH t_select AS (
                select date_trunc('month', fdate)::date fsignup_month,
                       fchannel_id,
                       sum(fcost_money) fcost_money
                  from analysis.marketing_cpc_day_cost_info
                 where fdate >= date_trunc('month',current_date) - interval '9 month'
                   and fchannel_id is not null
                 group by date_trunc('month', fdate), fchannel_id
                 order by fsignup_month
               ),

            updated as ( UPDATE marketing_roi_cost_fct as target SET fcost_money = t_select.fcost_money  from t_select
              where target.fsignup_month = t_select.fsignup_month and target.fchannel_id = t_select.fchannel_id
              RETURNING target.fsignup_month, target.fchannel_id  )

            INSERT INTO marketing_roi_cost_fct(fsignup_month, fchannel_id, fcost_money)
            SELECT t_select.* FROM t_select
              LEFT JOIN updated on t_select.fsignup_month = updated.fsignup_month
               AND t_select.fchannel_id = updated.fchannel_id
             WHERE updated.fchannel_id IS NULL

         """ % self.sql_dict
        self.append(sql)

        sql = """
             WITH t_select AS (
             select date_trunc('month', date'%(ld_begin)s')::date fdate,
                   fchannel_id,
                   sum(fdip) fdip
              from analysis.marketing_channel_dims_fct
             where fdate >= date_trunc('month', date'%(ld_begin)s')
               and fdate < date'%(ld_end)s'
               and fchannel_id is not null
             group by fchannel_id
             order by fdate
               ),

            updated as ( UPDATE marketing_roi_cps_cost_fct as target SET fdip = t_select.fdip  from t_select
                          where target.fdate = t_select.fdate and target.fchannel_id = t_select.fchannel_id
                          RETURNING target.fdate, target.fchannel_id  )

            INSERT INTO marketing_roi_cps_cost_fct(fdate, fchannel_id, fdip)
            SELECT t_select.* FROM t_select
              LEFT JOIN updated on t_select.fdate = updated.fdate
               AND t_select.fchannel_id = updated.fchannel_id
             WHERE updated.fchannel_id IS NULL
        """ % self.sql_dict
        self.append(sql)


        # 启动
        sql = """
             WITH t_select AS (

            select date_trunc('month', date'%(ld_begin)s')::date fsignup_month, fchannel_id,
                        sum(a.fdsu_d) fdsu_d,
                        sum(a.fdstart_d) fdstart_d
                   from analysis.marketing_channel_dims_fct a
                  where fdate >= date_trunc('month', date'%(ld_begin)s')
                    and fdate < date'%(ld_end)s'
                    and fchannel_id is not null
                    and a.fdstart_d > 0
                    group by fchannel_id

               ),

            updated as ( UPDATE marketing_roi_start_cost_fct as target SET fdsu_d = t_select.fdsu_d, fdstart_d = t_select.fdstart_d
                           from t_select
                          where target.fsignup_month = t_select.fsignup_month and target.fchannel_id = t_select.fchannel_id
                          RETURNING target.fsignup_month, target.fchannel_id  )

            INSERT INTO marketing_roi_start_cost_fct(fsignup_month, fchannel_id, fdsu_d, fdstart_d)
            SELECT t_select.* FROM t_select
              LEFT JOIN updated on t_select.fsignup_month = updated.fsignup_month
               AND t_select.fchannel_id = updated.fchannel_id
             WHERE updated.fchannel_id IS NULL
        """ % self.sql_dict
        self.append(sql)

        self.exe_hql_list()


if __name__ == "__main__":

    stat_date = get_stat_date()
    #生成统计实例
    a = new_marketing_ROI_cost_money_pg(stat_date)
    a()
