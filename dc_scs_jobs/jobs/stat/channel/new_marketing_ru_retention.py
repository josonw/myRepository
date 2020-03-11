#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class new_marketing_ru_retention(BaseStat):

    def create_tab(self):
        hql = """
        -- 特殊同步要求, 90天
        create table if not exists analysis.user_newchannel_retention
        (
          fdate         date,
          fchannel_id   varchar(100),
          fdru_day      int,
          fdru_num_type int,
          fdru_d_num    decimal(25,2)
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        # self.hq.debug = 1

        hql_list = []

        #加上当天的分区
        hql = """
        use analysis;
        alter table user_newchannel_retention add if not exists partition (dt='%(ld_begin)s');

        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- num_type=1 表示新增用户活跃留存

        drop table if exists stage.user_newchannel_retention_tmp1_%(num_begin)s;

        create table if not exists stage.user_newchannel_retention_tmp1_%(num_begin)s as
        select c.dt fdate,
                a.fchannel_id,
                datediff('%(ld_begin)s', c.dt) fdru_day,
                1 fdru_num_type,
                count(distinct c.fudid) fdru_d_num
           from stage.channel_market_active_mid a
           join stage.channel_market_new_reg_mid c
             on a.fbpid = c.fbpid
            and a.fudid = c.fudid
            and c.dt >= '%(ld_90dayago)s'
            and c.dt < '%(ld_end)s'
          where a.dt = '%(ld_begin)s'
          group by c.dt, a.fchannel_id, datediff('%(ld_begin)s', c.dt);
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- fdru_num_type=2 表示新增留存付费金额

        drop table if exists stage.user_newchannel_retention_tmp2_%(num_begin)s;

        create table if not exists stage.user_newchannel_retention_tmp2_%(num_begin)s as
        select c.dt fdate,
               a.fchannel_id,
               datediff('%(ld_begin)s', c.dt) fdru_day,
               2 fdru_num_type,
               round(sum(a.fpay_money * fpay_rate / coalesce(rate.rate, 0.157176) ), 2) fdru_d_num
          from stage.channel_market_payment_mid a
          join stage.channel_market_new_reg_mid c
            on a.fbpid = c.fbpid
           and a.fudid = c.fudid
           and c.dt >= '%(ld_90dayago)s'
           and c.dt < '%(ld_end)s'
        LEFT JOIN (
            select * from stage.paycenter_rate rate
            where
                rate.dt='%(ld_begin)s' AND rate.unit='CNY'
            limit 1
        ) rate
            ON rate.dt='%(ld_begin)s' AND rate.unit='CNY'
         where a.dt = '%(ld_begin)s'
         group by c.dt, a.fchannel_id, datediff('%(ld_begin)s', c.dt)
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_newchannel_retention partition(dt)
        select
            coalesce(b.fdate, a.fdate),
            coalesce(b.fchannel_id, a.fchannel_id),
            coalesce(b.fdru_day, a.fdru_day),
            coalesce(b.fdru_num_type, a.fdru_num_type),
            coalesce(b.fdru_d_num, a.fdru_d_num),
            coalesce(b.fdate, a.fdate) dt
        from (
            select * from analysis.user_newchannel_retention
             where dt >= '%(ld_90dayago)s'
               and dt < '%(ld_end)s' ) a
        full outer join (
            select * from stage.user_newchannel_retention_tmp1_%(num_begin)s
            union all
            select * from stage.user_newchannel_retention_tmp2_%(num_begin)s
            ) b
         on a.fdate            = b.fdate
        and a.fchannel_id      = b.fchannel_id
        and a.fdru_day         = b.fdru_day
        and a.fdru_num_type    = b.fdru_num_type

        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists stage.user_newchannel_retention_tmp1_%(num_begin)s;

        drop table if exists stage.user_newchannel_retention_tmp2_%(num_begin)s;
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_newchannel_retention partition(dt='3000-01-01')
        select
        fdate,
        fchannel_id,
        fdru_day,
        fdru_num_type,
        fdru_d_num
        from analysis.user_newchannel_retention
       where dt >= '%(ld_90dayago)s'
         and dt < '%(ld_end)s'
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
    a = new_marketing_ru_retention(stat_date)
    a()