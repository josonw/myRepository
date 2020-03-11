#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date

class new_marketing_ROI_cost_money_pg(BasePGCluster):
    """处理用户设置的单价和分成比例
    """

    def stat(self):
        sql = """delete from analysis.marketing_roi_cost_fct where fsignup_month >= date_trunc('month', date'%(ld_begin)s')::date and fsignup_month < date'%(ld_begin)s';

        delete from analysis.marketing_roi_cost_fct_tmp where fsignup_month >= date_trunc('month', date'%(ld_begin)s')::date and fsignup_month < date'%(ld_begin)s';

        delete from analysis.marketing_roi_cps_cost_fct where fdate >= date_trunc('month', date'%(ld_begin)s')::date and fdate < date'%(ld_begin)s';

        delete from analysis.marketing_roi_cps_cost_fct_tmp where fdate >= date_trunc('month', date'%(ld_begin)s')::date and fdate < date'%(ld_begin)s';

        delete from analysis.marketing_roi_cps_cost_fct_tmp_2 where fdate >= date_trunc('month', date'%(ld_begin)s')::date and fdate < date'%(ld_begin)s';

        delete from analysis.marketing_roi_start_cost_fct where fsignup_month >= date_trunc('month', date'%(ld_begin)s')::date and fsignup_month < date'%(ld_begin)s';

        delete from analysis.marketing_roi_start_cost_fct_tmp where fsignup_month >= date_trunc('month', date'%(ld_begin)s')::date and fsignup_month < date'%(ld_begin)s';

        delete from analysis.marketing_roi_start_cost_fct_tmp_2 where fsignup_month >= date_trunc('month', date'%(ld_begin)s')::date and fsignup_month < date'%(ld_begin)s';

        """ % self.sql_dict
        self.append(sql)



        sql = """
            INSERT INTO analysis.marketing_roi_cost_fct_tmp(fsignup_month, fchannel_id, fdsu_d)
         select date_trunc('month', date'%(ld_begin)s')::date fsignup_month, fchannel_id, sum(fdsu_d) fdsu_d
             from (
             select fdate,
                      a.fchannel_id,
                      row_number() over(partition by fdate, a.fchannel_id order by a.fdate - COALESCE(b.fchange_time, a.fdate)) rown,
                      round(fdsu_d * (1 - COALESCE(b.fdiscount, 0)), 0) fdsu_d
                 from analysis.marketing_channel_dims_fct a
                 left join analysis.marketing_channel_pkg_discount b
                   on a.fchannel_id = cast (b.fpkg_id as varchar)
                  and a.fdate > COALESCE(b.fchange_time, a.fdate)
                where fdate >=  date_trunc('month', date'%(ld_begin)s')
                  and fdate < date'%(ld_end)s'
                order by fdate
                ) as abc
            where rown = 1
            and fchannel_id is not null
            group by fchannel_id ;

        """ % self.sql_dict
        self.append(sql)


        sql = """
            INSERT INTO analysis.marketing_roi_cost_fct_tmp(fsignup_month, fchannel_id, fcost_money)
                select date_trunc('month', fdate)::date fsignup_month,
                       fchannel_id,
                       sum(fcost_money) fcost_money
                  from analysis.marketing_cpc_day_cost_info
                 where fdate >= date_trunc('month',current_date) - interval '9 month'
                   and fchannel_id is not null
                 group by date_trunc('month', fdate), fchannel_id
                 order by fsignup_month;

         """ % self.sql_dict
        self.append(sql)

        # 汇总
        sql = """
            INSERT INTO analysis.marketing_roi_cost_fct(fsignup_month,fchannel_id,fdsu_d,fprice,fcost_money)
            select fsignup_month,
                   fchannel_id,
                   sum(fdsu_d) fdsu_d,
                   sum(fprice) fprice,
                   sum(fcost_money) fcost_money
                   from analysis.marketing_roi_cost_fct_tmp a
                  where fsignup_month >= date_trunc('month', date'%(ld_begin)s')
                    and fsignup_month < date'%(ld_end)s'
                    group by fsignup_month,fchannel_id;
        """ % self.sql_dict
        self.append(sql)

        sql = """
             insert into analysis.marketing_roi_cps_cost_fct_tmp
             select date_trunc('month', date'%(ld_begin)s')::date fdate,
                   fchannel_id,
                   sum(fdip) fdip
              from analysis.marketing_channel_dims_fct
             where fdate >= date_trunc('month', date'%(ld_begin)s')
               and fdate < date'%(ld_end)s'
               and fchannel_id is not null
             group by fchannel_id
             order by fdate;

             insert into analysis.marketing_roi_cps_cost_fct_tmp_2
             SELECT target.fdate,target.fchannel_id,coalesce(t_select.fdip,target.fdip) fdip,target.fdivide_rate,target.fcost_money
                          FROM analysis.marketing_roi_cps_cost_fct as target
                          LEFT JOIN analysis.marketing_roi_cps_cost_fct_tmp as t_select
                            on target.fdate = t_select.fdate
                           and target.fchannel_id = t_select.fchannel_id
                         where target.fdate >= date_trunc('month', date'%(ld_begin)s')
                           and target.fdate < date'%(ld_end)s';

            INSERT INTO analysis.marketing_roi_cps_cost_fct(fdate, fchannel_id, fdip,fdivide_rate,fcost_money)
            SELECT updated.* FROM analysis.marketing_roi_cps_cost_fct_tmp_2 as updated;

        """ % self.sql_dict
        self.append(sql)


        # 启动
        sql = """
            insert into analysis.marketing_roi_start_cost_fct_tmp
              select date_trunc('month', date'%(ld_begin)s')::date fsignup_month, fchannel_id,
                        sum(a.fdsu_d) fdsu_d,
                        sum(a.fdstart_d) fdstart_d
                   from analysis.marketing_channel_dims_fct a
                  where fdate >= date_trunc('month', date'%(ld_begin)s')
                    and fdate < date'%(ld_end)s'
                    and fchannel_id is not null
                    and a.fdstart_d > 0
                    group by fchannel_id;

             insert into analysis.marketing_roi_start_cost_fct_tmp_2
             SELECT target.fsignup_month,target.fchannel_id,coalesce(t_select.fdsu_d,target.fdsu_d) fdsu_d,coalesce(t_select.fdstart_d,target.fdstart_d) fdstart_d
                          FROM analysis.marketing_roi_start_cost_fct as target
                          LEFT JOIN analysis.marketing_roi_start_cost_fct_tmp as t_select
                            on target.fsignup_month = t_select.fsignup_month
                           and target.fchannel_id = t_select.fchannel_id
                         where target.fsignup_month >= date_trunc('month', date'%(ld_begin)s')
                           and target.fsignup_month < date'%(ld_end)s';

            INSERT INTO analysis.marketing_roi_start_cost_fct(fsignup_month, fchannel_id, fdsu_d, fdstart_d)
            SELECT t_select.* FROM analysis.marketing_roi_start_cost_fct_tmp_2 as t_select;
        """ % self.sql_dict
        self.append(sql)

        self.exe_hql_list()


if __name__ == "__main__":

    stat_date = get_stat_date()
    #生成统计实例
    a = new_marketing_ROI_cost_money_pg(stat_date)
    a()
