#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class PushUserPayInfo(BaseStatModel):
    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS bud_dm.bud_push_user_pay_info(
              fdate date,
              fpush_id int COMMENT '推送id',
              pre_paid_post_pay_persons int COMMENT '推送前付费推送后继续付费人数',
              pre_unpaid_post_pay_persons int COMMENT '推送前未付费推送后付费人数',
              post_first_pay_persons_day1 int COMMENT '推送后当日首次付费人数',
              post_first_pay_persons_day2 int COMMENT '推送后第2日首次付费人数',
              post_first_pay_persons_day3 int COMMENT '推送后第3日首次付费人数',
              post_first_pay_persons_day4 int COMMENT '推送后第4日首次付费人数',
              post_first_pay_persons_day5 int COMMENT '推送后第5日首次付费人数',
              post_first_pay_persons_day6 int COMMENT '推送后第6日首次付费人数',
              post_first_pay_persons_day7 int COMMENT '推送后第7日首次付费人数',
              post_pay_persons_day1 int COMMENT '推送后当日付费人数',
              post_pay_persons_day2 int COMMENT '推送后第2日付费人数',
              post_pay_persons_day3 int COMMENT '推送后第3日付费人数',
              post_pay_persons_day4 int COMMENT '推送后第4日付费人数',
              post_pay_persons_day5 int COMMENT '推送后第5日付费人数',
              post_pay_persons_day6 int COMMENT '推送后第6日付费人数',
              post_pay_persons_day7 int COMMENT '推送后第7日付费人数',
              post_pay_money_day1 decimal(12,2) COMMENT '推送后当日付费额度',
              post_pay_money_day2 decimal(12,2) COMMENT '推送后第2日付费额度',
              post_pay_money_day3 decimal(12,2) COMMENT '推送后第3日付费额度',
              post_pay_money_day4 decimal(12,2) COMMENT '推送后第4日付费额度',
              post_pay_money_day5 decimal(12,2) COMMENT '推送后第5日付费额度',
              post_pay_money_day6 decimal(12,2) COMMENT '推送后第6日付费额度',
              post_pay_money_day7 decimal(12,2) COMMENT '推送后第7日付费额度',
              post_pay_orders_day1 int COMMENT '推送后当日付费次数',
              post_pay_orders_day2 int COMMENT '推送后第2日付费次数',
              post_pay_orders_day3 int COMMENT '推送后第3日付费次数',
              post_pay_orders_day4 int COMMENT '推送后第4日付费次数',
              post_pay_orders_day5 int COMMENT '推送后第5日付费次数',
              post_pay_orders_day6 int COMMENT '推送后第6日付费次数',
              post_pay_orders_day7 int COMMENT '推送后第7日付费次数',
              post_pay_persons int COMMENT '推送后7日总付费人数',
              post_pay_money decimal(12,2) COMMENT '推送后7日总付费额度',
              post_pay_orders int COMMENT '推送后7日总付费次数',
              pay_proportion decimal(5,4) COMMENT '推送后7日付费转化率')
            COMMENT '推送后用户付费情况'
            partitioned by (dt string)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            with first_pay_info as
            (
                select t1.fpush_id, t2.fuid, min(t3.dt) as first_pay_date
                  from stage.push_config_stg t1
                 inner join dim.user_push_report t2
                    on t1.fpush_id = t2.fpush_id
                   and t2.faction = 1
                  left join stage.payment_stream_stg t3
                    on (t1.fbpid = t3.fbpid and t2.fuid = t3.fuid)
                 where date_add(t1.fbegin_time, 7) = '%(ld_1day_ago)s'
                   and t1.ftoken_id = 0
                 group by t1.fpush_id, t2.fuid
            ),
            target_users_pay_info as
            (
                select t1.fpush_id,
                       t2.fuid,
                       to_date(t1.fbegin_time) as push_date,
                       count(t3.forder_id) as pay_orders,
                       sum(t3.fcoins_num) as pay_money,
                       t3.dt as pay_date
                  from stage.push_config_stg t1
                 inner join dim.user_push_report t2
                    on t1.fpush_id = t2.fpush_id
                   and t2.faction = 1
                  left join stage.payment_stream_stg t3
                    on (t1.fbpid = t3.fbpid and t2.fuid = t3.fuid)
                 where t3.dt >= to_date(t1.fbegin_time)
                   and t3.dt < date_add(t1.fbegin_time, 7)
                   and date_add(t1.fbegin_time, 7) = '%(ld_1day_ago)s'
                   and t1.ftoken_id = 0
                 group by t1.fpush_id, t2.fuid, to_date(t1.fbegin_time), t3.dt
            )
            insert overwrite table bud_dm.bud_push_user_pay_info partition (dt='%(ld_1day_ago)s')
            select '%(ld_1day_ago)s' fdate,
              v2.fpush_id,
              count(distinct(case when v1.first_pay_date<v2.push_date and v2.pay_date>=v2.push_date then v2.fuid else null end)) pre_paid_post_pay_persons,
              count(distinct(case when v1.first_pay_date>=v2.push_date then v2.fuid else null end)) as pre_unpaid_post_pay_persons,
              count(distinct(case when v1.first_pay_date=v2.push_date then v2.fuid else null end)) as post_first_pay_persons_day1,
              count(distinct(case when v1.first_pay_date=date_add(v2.push_date,1) then v2.fuid else null end)) as post_first_pay_persons_day2,
              count(distinct(case when v1.first_pay_date=date_add(v2.push_date,2) then v2.fuid else null end)) as post_first_pay_persons_day3,
              count(distinct(case when v1.first_pay_date=date_add(v2.push_date,3) then v2.fuid else null end)) as post_first_pay_persons_day4,
              count(distinct(case when v1.first_pay_date=date_add(v2.push_date,4) then v2.fuid else null end)) as post_first_pay_persons_day5,
              count(distinct(case when v1.first_pay_date=date_add(v2.push_date,5) then v2.fuid else null end)) as post_first_pay_persons_day6,
              count(distinct(case when v1.first_pay_date=date_add(v2.push_date,6) then v2.fuid else null end)) as post_first_pay_persons_day7,
              count(distinct(case when v2.pay_date=v2.push_date then v2.fuid else null end)) as post_pay_persons_day1,
              count(distinct(case when v2.pay_date=date_add(v2.push_date,1) then v2.fuid else null end)) as post_pay_persons_day2,
              count(distinct(case when v2.pay_date=date_add(v2.push_date,2) then v2.fuid else null end)) as post_pay_persons_day3,
              count(distinct(case when v2.pay_date=date_add(v2.push_date,3) then v2.fuid else null end)) as post_pay_persons_day4,
              count(distinct(case when v2.pay_date=date_add(v2.push_date,4) then v2.fuid else null end)) as post_pay_persons_day5,
              count(distinct(case when v2.pay_date=date_add(v2.push_date,5) then v2.fuid else null end)) as post_pay_persons_day6,
              count(distinct(case when v2.pay_date=date_add(v2.push_date,6) then v2.fuid else null end)) as post_pay_persons_day7,
              sum(case when v2.pay_date=v2.push_date then v2.pay_money else 0 end) as post_pay_money_day1,
              sum(case when v2.pay_date=date_add(v2.push_date,1) then v2.pay_money else 0 end) as post_pay_money_day2,
              sum(case when v2.pay_date=date_add(v2.push_date,2) then v2.pay_money else 0 end) as post_pay_money_day3,
              sum(case when v2.pay_date=date_add(v2.push_date,3) then v2.pay_money else 0 end) as post_pay_money_day4,
              sum(case when v2.pay_date=date_add(v2.push_date,4) then v2.pay_money else 0 end) as post_pay_money_day5,
              sum(case when v2.pay_date=date_add(v2.push_date,5) then v2.pay_money else 0 end) as post_pay_money_day6,
              sum(case when v2.pay_date=date_add(v2.push_date,6) then v2.pay_money else 0 end) as post_pay_money_day7,
              count(case when v2.pay_date=v2.push_date then v2.pay_orders else null end) as post_pay_orders_day1,
              count(case when v2.pay_date=date_add(v2.push_date,1) then v2.pay_orders else null end) as post_pay_orders_day2,
              count(case when v2.pay_date=date_add(v2.push_date,2) then v2.pay_orders else null end) as post_pay_orders_day3,
              count(case when v2.pay_date=date_add(v2.push_date,3) then v2.pay_orders else null end) as post_pay_orders_day4,
              count(case when v2.pay_date=date_add(v2.push_date,4) then v2.pay_orders else null end) as post_pay_orders_day5,
              count(case when v2.pay_date=date_add(v2.push_date,5) then v2.pay_orders else null end) as post_pay_orders_day6,
              count(case when v2.pay_date=date_add(v2.push_date,6) then v2.pay_orders else null end) as post_pay_orders_day7,
              count(distinct(case when v2.pay_money>0 then v2.fuid else null end)) as post_pay_persons,
              sum(v2.pay_money) as post_pay_money,
              count(v2.pay_orders) as post_pay_orders,
              count(distinct(case when v2.pay_money>0 then v2.fuid else null end))/count(distinct v2.fuid) as pay_proportion
            from first_pay_info v1
            right join target_users_pay_info v2
            on (v1.fpush_id = v2.fpush_id and v1.fuid = v2.fuid)
            group by v2.fpush_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
a = PushUserPayInfo(sys.argv[1:])
a()
