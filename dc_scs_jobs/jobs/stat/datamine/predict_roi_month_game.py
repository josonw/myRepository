#-*- coding: UTF-8 -*- 
import datetime
import numpy as np
import pandas as pd
from pandas import Series, DataFrame
import math
import os
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStat import BasePGStat, get_stat_date
from PublicFunc import PublicFunc

class predict_roi_month_game(BasePGStat):
    """结果运算"""
    def stat(self):
        self.sql_dict = PublicFunc.date_define(self.stat_date)
        # 删除当月数据
        sql = """ 
            delete from analysis.prediction_roi_fct
             where fdate >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd') 
               and fdate < to_date('%(ld_monthend)s', 'yyyy-mm-dd')""" % self.sql_dict
        self.exe_hql(sql)

        sql = """ 
            insert into analysis.prediction_roi_fct
          (fdate, fgame_id, fgame, fbelong_group, fdru_month, fsignup_month)
            select t1.fdate,
                   t1.fgame_id,
                   t1.fgame,
                   t1.fbelong_group,
                   t2.fdru_month,
                   t1.fsignup_month
              from analysis.prediction_roi_mid t1
              join (with recursive cte(n) AS (
                              values (1)
                            union all
                              select n+1 from cte where n <= 12
                          )
                          select n-1 fdru_month from cte) t2
                      on 1 = 1 
              where fdate >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd')
                and fdate < to_date('%(ld_monthend)s', 'yyyy-mm-dd') """ % self.sql_dict
        self.exe_hql(sql)

        sql = """ 
        update analysis.prediction_roi_fct t1
           set fcost = coalesce(t2.fcost, 0)
          from (select a.fsignup_month fsignup_month,
                       b.fgame_id fgame_id,
                           c.fbelong_group,
                           round(sum(case when fcost_money > 0 then fcost_money else fdsu_d * a.fprice end), 2) fcost
                     from analysis.marketing_roi_cost_fct a
                     join analysis.marketing_channel_pkg_info b
                       on a.fchannel_id = to_char(b.fid)
                     left join analysis.marketing_channel_trader_info c
                       on b.ftrader_id = c.fid
                     join analysis.marketing_game_info d
                       on b.fgame_id = d.fid
                    where b.fstate in (1, 2)
                      and (b.fcoop_type like '%%%%CPA%%%%' or fcoop_type like '%%%%CPC%%%%')
                    group by a.fsignup_month, b.fgame_id, c.fbelong_group) t2
              where t1.fsignup_month = t2.fsignup_month 
                  and t1.fgame_id = t2.fgame_id 
                  and t1.fbelong_group = t2.fbelong_group 
                  and t1.fdate >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd')
                  and t1.fdate < to_date('%(ld_monthend)s', 'yyyy-mm-dd') """ % self.sql_dict
        self.exe_hql(sql)

        sql = """ 
            update analysis.prediction_roi_fct t1
               set fis_pred = 0, fincome = coalesce(t2.fincome, 0)
                from (select a.fsignup_month,
                          b.fgame_id fgame_id,
                          c.fbelong_group fbelong_group,
                          sum(coalesce(fdip, 0)) fincome,
                          fdru_month,
                          0 fis_pred
                     from analysis.marketing_roi_retain_pay a
                     join analysis.marketing_channel_pkg_info b
                       on a.fchannel_id = to_char(b.fid)
                     left join analysis.marketing_channel_trader_info c
                       on b.ftrader_id = c.fid
                     join analysis.marketing_game_info d
                       on b.fgame_id = d.fid
                    where b.fstate in (1, 2)
                      and (b.fcoop_type like '%%%%CPA%%%%' or fcoop_type like '%%%%CPC%%%%')
                      and a.fsignup_month + ((fdru_month || ' month')::interval) < to_date('%(ld_monthbegin)s', 'yyyy-mm-dd')             
                    group by a.fsignup_month, b.fgame_id, fdru_month, c.fbelong_group
                    ) t2
            where t1.fsignup_month = t2.fsignup_month 
                and t1.fgame_id = t2.fgame_id 
                and t1.fbelong_group = t2.fbelong_group 
                and t1.fdru_month = t2.fdru_month 
                and t1.fdate >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd')
                and t1.fdate < to_date('%(ld_monthend)s', 'yyyy-mm-dd') """ % self.sql_dict
        self.exe_hql(sql)

        # 预测收入标识
        sql = """ 
              update analysis.prediction_roi_fct
                 set fis_pred = 1
               where fsignup_month + ((fdru_month || ' month')::interval) >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd')
            """ % self.sql_dict
        self.exe_hql(sql)

        sql = """ 
              update analysis.prediction_roi_fct t1
                 set fincome = case when t2.fexp_slope = 'NaN' then 0 
                                else coalesce(t1.fcost * t2.ffirst_roi * exp(t2.fexp_slope * t1.fdru_month), 0) end
              from analysis.prediction_roi_mid t2
             where t1.fsignup_month = t2.fsignup_month 
               and t1.fgame_id = t2.fgame_id 
               and t1.fbelong_group = t2.fbelong_group 
               and fis_pred = 1 
               and t1.fdate >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd')
               and t1.fdate < to_date('%(ld_monthend)s', 'yyyy-mm-dd')
               and t1.fdate = t2.fdate  """ % self.sql_dict
        self.exe_hql(sql)

        sql = """
              update analysis.prediction_roi_fct t1
                 set ftotal_income = coalesce(t2.ftotal_income, 0)
                from (select fdate,
                          fgame_id,
                          fbelong_group,
                          fsignup_month,
                          fdru_month,
                          fcost,
                          fincome,
                          sum(coalesce(fincome, 0)) over(partition by fgame_id, fbelong_group, fsignup_month order by fdru_month) ftotal_income
                     from analysis.prediction_roi_fct
                    where fdate >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd')
                      and fdate < to_date('%(ld_monthend)s', 'yyyy-mm-dd')) t2
            where t1.fsignup_month = t2.fsignup_month 
              and t1.fgame_id = t2.fgame_id 
              and t1.fbelong_group = t2.fbelong_group 
              and t1.fdru_month = t2.fdru_month 
              and t1.fdate >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd')
              and t1.fdate < to_date('%(ld_monthend)s', 'yyyy-mm-dd')
              and t1.fdate = t2.fdate""" % self.sql_dict
        self.exe_hql(sql)

        sql = """ 
              update analysis.prediction_roi_fct
                 set ftotal_roi = coalesce(case when fcost = 0 then 0 else ftotal_income / fcost end, 0)
               where fdate >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd')
                 and fdate < to_date('%(ld_monthend)s', 'yyyy-mm-dd')""" % self.sql_dict
        self.exe_hql(sql)


if __name__ == '__main__':
    stat_date = get_stat_date()
    p = predict_roi_month_game(stat_date)
    p()
    


