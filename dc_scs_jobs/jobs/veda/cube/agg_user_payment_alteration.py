#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserPaymentAlteration(BaseStatModel):
    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS veda.user_payment_alteration
            (
              fgamefsk BIGINT COMMENT '游戏ID',
              fgamename STRING COMMENT '游戏名称',
              fplatformfsk BIGINT COMMENT '平台ID',
              fplatformname STRING COMMENT '平台名称',
              fuid BIGINT COMMENT '用户ID',
              payment_amount_avg60day DECIMAL(10,2) COMMENT '近60日平均付费（原币）',
              payment_amount_avg30day DECIMAL(10,2) COMMENT '近30日平均付费（原币）',
              payment_amount_avg15day DECIMAL(10,2) COMMENT '近15日平均付费（原币）',
              payment_amount_avg7day DECIMAL(10,2) COMMENT '近7日平均付费（原币）',
              payment_amount_avg60day_usd DECIMAL(10,2) COMMENT '近60日平均付费（美元）',
              payment_amount_avg30day_usd DECIMAL(10,2) COMMENT '近30日平均付费（美元）',
              payment_amount_avg15day_usd DECIMAL(10,2) COMMENT '近15日平均付费（美元）',
              payment_amount_avg7day_usd DECIMAL(10,2) COMMENT '近7日平均付费（美元）',
              trend STRING COMMENT '付费趋势',
              alteration DECIMAL(3,2) COMMENT '付费变化率'
            )
            COMMENT '用户付费变化情况'
            STORED AS PARQUET
        """
        res = self.sql_exe(hql)
        return res

    def stat(self):
        hql = """
            with v as
             (select fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fuid,
                     sum(case
                           when dt >= date_sub('%(statdate)s', 59) and dt <= '%(statdate)s' then
                            pay_sum
                           else
                            0
                         end) / 60 as payment_amount_avg60day,
                     sum(case
                           when dt >= date_sub('%(statdate)s', 29) and dt <= '%(statdate)s' then
                            pay_sum
                           else
                            0
                         end) / 30 as payment_amount_avg30day,
                     sum(case
                           when dt >= date_sub('%(statdate)s', 14) and dt <= '%(statdate)s' then
                            pay_sum
                           else
                            0
                         end) / 15 as payment_amount_avg15day,
                     sum(case
                           when dt >= date_sub('%(statdate)s', 6) and dt <= '%(statdate)s' then
                            pay_sum
                           else
                            0
                         end) / 7 as payment_amount_avg7day,
                     sum(case
                           when dt >= date_sub('%(statdate)s', 59) and dt <= '%(statdate)s' then
                            pay_sum_usd
                           else
                            0
                         end) / 60 as payment_amount_avg60day_usd,
                     sum(case
                           when dt >= date_sub('%(statdate)s', 29) and dt <= '%(statdate)s' then
                            pay_sum_usd
                           else
                            0
                         end) / 30 as payment_amount_avg30day_usd,
                     sum(case
                           when dt >= date_sub('%(statdate)s', 14) and dt <= '%(statdate)s' then
                            pay_sum_usd
                           else
                            0
                         end) / 15 as payment_amount_avg15day_usd,
                     sum(case
                           when dt >= date_sub('%(statdate)s', 6) and dt <= '%(statdate)s' then
                            pay_sum_usd
                           else
                            0
                         end) / 7 as payment_amount_avg7day_usd
                from veda.user_label_day
               where dt >= date_sub('%(statdate)s', 59)
                 and dt <= '%(statdate)s'
               group by fgamefsk, fgamename, fplatformfsk, fplatformname, fuid)
            insert overwrite table veda.user_payment_alteration
            select fgamefsk,
                   fgamename,
                   fplatformfsk,
                   fplatformname,
                   fuid,
                   payment_amount_avg60day,
                   payment_amount_avg30day,
                   payment_amount_avg15day,
                   payment_amount_avg7day,
                   payment_amount_avg60day_usd,
                   payment_amount_avg30day_usd,
                   payment_amount_avg15day_usd,
                   payment_amount_avg7day_usd,
                   concat(case
                            when payment_amount_avg60day < payment_amount_avg30day then
                             '↗'
                            when payment_amount_avg60day > payment_amount_avg30day then
                             '↘'
                            when payment_amount_avg60day = payment_amount_avg30day and
                                 payment_amount_avg60day != 0 then
                             '→'
                            else
                             ''
                          end,
                          case
                            when payment_amount_avg30day < payment_amount_avg15day then
                             '↗'
                            when payment_amount_avg30day > payment_amount_avg15day then
                             '↘'
                            when payment_amount_avg30day = payment_amount_avg15day and
                                 payment_amount_avg30day != 0 then
                             '→'
                            else
                             ''
                          end,
                          case
                            when payment_amount_avg15day < payment_amount_avg7day then
                             '↗'
                            when payment_amount_avg15day > payment_amount_avg7day then
                             '↘'
                            when payment_amount_avg15day = payment_amount_avg7day and
                                 payment_amount_avg15day != 0 then
                             '→'
                            else
                             ''
                          end) as trend,
                   round((payment_amount_avg30day - payment_amount_avg60day) /
                         payment_amount_avg60day * 0.3 +
                         nvl((payment_amount_avg15day - payment_amount_avg30day) /
                             payment_amount_avg30day,
                             -1) * 0.3 +
                         nvl((payment_amount_avg7day - payment_amount_avg15day) /
                             payment_amount_avg15day,
                             -1) * 0.4,
                         2) as alteration
              from v
             where payment_amount_avg60day > 0
        """
        res = self.sql_exe(hql)
        return res


# 实例化执行
a = UserPaymentAlteration(sys.argv[1:])
a()