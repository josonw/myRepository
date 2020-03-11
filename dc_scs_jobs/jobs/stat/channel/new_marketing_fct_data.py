#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class new_marketing_fct_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.marketing_channel_dims_fct
        (
          fdate         date,
          fchannel_id   varchar(64),
          fdsu_d        bigint,
          f7dsu_d       bigint,
          f30dsu_d      bigint,
          fdau_d        bigint,
          f7dau_d       bigint,
          f30dau_d      bigint,
          fdpu_d        bigint,
          f7dpu_d       bigint,
          f30dpu_d      bigint,
          fdip          decimal(20,2),
          f7dip         decimal(20,2),
          f30dip        decimal(20,2),
          fdsuip        decimal(20,2),
          f7dsuip       decimal(20,2),
          f30dsuip      decimal(20,2),
          fdstart_d     bigint,
          fdstart_d_num bigint,
          fdsu_d_num    bigint,
          fdau_d_cnt    bigint,
          fdsupu_d      bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result

    def stat(self):
        # self.hq.debug = 1

        hql_list = []
        hql = """
        -- 启动新增设备数
        drop table if exists analysis.marketing_fct_dstart_d_tmp_%(num_begin)s;
        create table if not exists analysis.marketing_fct_dstart_d_tmp_%(num_begin)s as
        select '%(ld_begin)s'  fdate,
                 fchannel_id,
                 count( distinct fudid ) fdstart_d
            from stage.channel_market_new_start_mid a
           where dt = '%(ld_begin)s'
           group by fchannel_id;

        -- 启动设备个数
        drop table if exists analysis.marketing_fct_dstart_d_num_tmp_%(num_begin)s;
        create table if not exists analysis.marketing_fct_dstart_d_num_tmp_%(num_begin)s as
        select '%(ld_begin)s' fdate,
        fchannel_id, count(distinct fudid) fdstart_d_num
        from stage.fd_user_channel_stg
        where dt = '%(ld_begin)s'
         and fet_id = 1
        group by fchannel_id;
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- 新增设备数
        drop table if exists analysis.marketing_fct_dsu_d_tmp_%(num_begin)s;
        create table if not exists analysis.marketing_fct_dsu_d_tmp_%(num_begin)s as
        select '%(ld_begin)s'  fdate,
                 fchannel_id,
                 count(distinct case when fsignup_at>='%(ld_begin)s'               and fsignup_at < '%(ld_end)s' then fudid end ) fdsu_d,
                 count(distinct case when fsignup_at>=date_add('%(ld_begin)s',-6)  and fsignup_at < '%(ld_end)s' then fudid end ) f7dsu_d,
                 count(distinct case when fsignup_at>=date_add('%(ld_begin)s',-29) and fsignup_at < '%(ld_end)s' then fudid end ) f30dsu_d
            from stage.channel_market_new_reg_mid
           where dt >= date_add('%(ld_begin)s', -29)
             and dt < '%(ld_end)s'
           group by fchannel_id;
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- 新增设备个数
        drop table if exists analysis.marketing_fct_dsu_d_num_tmp_%(num_begin)s;
        create table if not exists analysis.marketing_fct_dsu_d_num_tmp_%(num_begin)s as
        select '%(ld_begin)s' fdate,
                fnow_channel_id fchannel_id,
                count(distinct fudid) fdsu_d_num
           from stage.channel_market_reg_mid a
          where a.dt = '%(ld_begin)s'
            and fudid != ''
            and fudid is not null
          group by fnow_channel_id
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- 活跃设备
        drop table if exists analysis.marketing_fct_dau_d_tmp_%(num_begin)s;
        create table if not exists analysis.marketing_fct_dau_d_tmp_%(num_begin)s as
        select '%(ld_begin)s'  fdate,
                 fchannel_id,
                 count( distinct fudid ) fdau_d,
                 0 f7dau_d,
                 0 f30dau_d,
                 sum( factive_cnt ) fdau_d_cnt
            from stage.channel_market_active_mid
           where dt >= '%(ld_begin)s'
             and dt < '%(ld_end)s'
             and fudid != ''
             and fudid is not null
           group by fchannel_id
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- 付费设备数, 付费金额
        drop table if exists analysis.marketing_fct_dpu_dip_tmp_%(num_begin)s;
        create table if not exists analysis.marketing_fct_dpu_dip_tmp_%(num_begin)s as
        select '%(ld_begin)s' fdate, fchannel_id,
                 count(distinct case when a.fdate>='%(ld_begin)s'               and a.fdate < '%(ld_end)s' then a.fudid end ) fdpu_d,
                 count(distinct case when a.fdate>=date_add('%(ld_begin)s',-6)  and a.fdate < '%(ld_end)s' then a.fudid end ) f7dpu_d,
                 count(distinct case when a.fdate>=date_add('%(ld_begin)s',-29) and a.fdate < '%(ld_end)s' then a.fudid end ) f30dpu_d,

                 cast(round(sum(case when a.fdate>='%(ld_begin)s'               and a.fdate < '%(ld_end)s' then a.fpay_money * a.fpay_rate/coalesce(rate.rate, 0.157176) end ),2) as decimal(20,2) ) fdip,
                 cast(round(sum(case when a.fdate>=date_add('%(ld_begin)s',-6)  and a.fdate < '%(ld_end)s' then a.fpay_money * a.fpay_rate/coalesce(rate.rate, 0.157176) end ),2) as decimal(20,2) ) f7dip,
                 cast(round(sum(case when a.fdate>=date_add('%(ld_begin)s',-29) and a.fdate < '%(ld_end)s' then a.fpay_money * a.fpay_rate/coalesce(rate.rate, 0.157176) end ),2) as decimal(20,2) ) f30dip

            from stage.channel_market_payment_mid a
            LEFT JOIN (
                select * from stage.paycenter_rate rate
                where
                    rate.dt='%(ld_begin)s' AND rate.unit='CNY'
                limit 1
            ) rate
                ON rate.dt='%(ld_begin)s' AND rate.unit='CNY'
           where a.dt>= date_add('%(ld_begin)s', -29)
             and a.dt < '%(ld_end)s'
          group By fchannel_id
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- 新增用户的付费金额
        drop table if exists analysis.marketing_fct_dsuip_tmp_%(num_begin)s;
        create table if not exists analysis.marketing_fct_dsuip_tmp_%(num_begin)s as
        select  '%(ld_begin)s' fdate, a.fchannel_id,
            count(distinct case when a.fsignup_at >= '%(ld_begin)s'               and a.fsignup_at < '%(ld_end)s' then a.fudid end) fdsupu_d,
            cast(round(sum(case when a.fsignup_at >= '%(ld_begin)s'               and a.fsignup_at < '%(ld_end)s' then b.fpay_money * b.fpay_rate/coalesce(rate.rate, 0.157176) end ),2) as decimal(20,2) ) fdsuip,
            cast(round(sum(case when a.fsignup_at >= date_add('%(ld_begin)s',-6)  and a.fsignup_at < '%(ld_end)s' then b.fpay_money * b.fpay_rate/coalesce(rate.rate, 0.157176) end ),2) as decimal(20,2) ) f7dsuip,
            cast(round(sum(case when a.fsignup_at >= date_add('%(ld_begin)s',-29) and a.fsignup_at < '%(ld_end)s' then b.fpay_money * b.fpay_rate/coalesce(rate.rate, 0.157176) end ),2) as decimal(20,2) ) f30dsuip

        from stage.channel_market_new_reg_mid a
        join stage.channel_market_payment_mid b
          on a.fbpid=b.fbpid
         and a.fudid=b.fudid
         and a.dt >= date_add('%(ld_begin)s', -29)
         and a.dt <  '%(ld_end)s'
         and b.dt >= date_add('%(ld_begin)s', -29)
         and b.dt <  '%(ld_end)s'
         and a.dt = b.dt
        LEFT JOIN (
            select * from stage.paycenter_rate rate
            where
                rate.dt='%(ld_begin)s' AND rate.unit='CNY'
            limit 1
        ) rate
            ON rate.dt='%(ld_begin)s' AND rate.unit='CNY'
       where a.fudid != ''
         and a.fudid is not null
       group By a.fchannel_id
       """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.marketing_fct_tmp_%(num_begin)s;
        create table if not exists analysis.marketing_fct_tmp_%(num_begin)s as
        select *
        from (
            select fdate,
            fchannel_id,
                   null        fdsu_d,
                   null        f7dsu_d,
                   null        f30dsu_d,
                   null        fdau_d,
                   null        f7dau_d,
                   null        f30dau_d,
                   null        fdpu_d,
                   null        f7dpu_d,
                   null        f30dpu_d,
                   null        fdip,
                   null        f7dip,
                   null        f30dip,
                   null        fdsuip,
                   null        f7dsuip,
                   null        f30dsuip,
                               fdstart_d,
                   null        fdstart_d_num,
                   null        fdsu_d_num,
                   null        fdau_d_cnt,
                   null        fdsupu_d
              from analysis.marketing_fct_dstart_d_tmp_%(num_begin)s a
            union all
            select fdate,
                   fchannel_id,
                   null        fdsu_d,
                   null        f7dsu_d,
                   null        f30dsu_d,
                   null        fdau_d,
                   null        f7dau_d,
                   null        f30dau_d,
                   null        fdpu_d,
                   null        f7dpu_d,
                   null        f30dpu_d,
                   null        fdip,
                   null        f7dip,
                   null        f30dip,
                   null        fdsuip,
                   null        f7dsuip,
                   null        f30dsuip,
                   null        fdstart_d,
                               fdstart_d_num,
                   null        fdsu_d_num,
                   null        fdau_d_cnt,
                   null        fdsupu_d
              from analysis.marketing_fct_dstart_d_num_tmp_%(num_begin)s b
            union all
            select fdate,
                   fchannel_id,
                               fdsu_d,
                               f7dsu_d,
                               f30dsu_d,
                   null        fdau_d,
                   null        f7dau_d,
                   null        f30dau_d,
                   null        fdpu_d,
                   null        f7dpu_d,
                   null        f30dpu_d,
                   null        fdip,
                   null        f7dip,
                   null        f30dip,
                   null        fdsuip,
                   null        f7dsuip,
                   null        f30dsuip,
                   null        fdstart_d,
                   null        fdstart_d_num,
                   null        fdsu_d_num,
                   null        fdau_d_cnt,
                   null        fdsupu_d
              from analysis.marketing_fct_dsu_d_tmp_%(num_begin)s c
            union all
            select fdate,
                   fchannel_id,
                   null        fdsu_d,
                   null        f7dsu_d,
                   null        f30dsu_d,
                   null        fdau_d,
                   null        f7dau_d,
                   null        f30dau_d,
                   null        fdpu_d,
                   null        f7dpu_d,
                   null        f30dpu_d,
                   null        fdip,
                   null        f7dip,
                   null        f30dip,
                   null        fdsuip,
                   null        f7dsuip,
                   null        f30dsuip,
                   null        fdstart_d,
                   null        fdstart_d_num,
                               fdsu_d_num,
                   null        fdau_d_cnt,
                   null        fdsupu_d
              from analysis.marketing_fct_dsu_d_num_tmp_%(num_begin)s d
            union all
            select fdate,
                   fchannel_id,
                   null        fdsu_d,
                   null        f7dsu_d,
                   null        f30dsu_d,
                               fdau_d,
                               f7dau_d,
                               f30dau_d,
                   null        fdpu_d,
                   null        f7dpu_d,
                   null        f30dpu_d,
                   null        fdip,
                   null        f7dip,
                   null        f30dip,
                   null        fdsuip,
                   null        f7dsuip,
                   null        f30dsuip,
                   null        fdstart_d,
                   null        fdstart_d_num,
                   null        fdsu_d_num,
                               fdau_d_cnt,
                   null        fdsupu_d
              from analysis.marketing_fct_dau_d_tmp_%(num_begin)s e
            union all
            select fdate,
                   fchannel_id,
                   null        fdsu_d,
                   null        f7dsu_d,
                   null        f30dsu_d,
                   null        fdau_d,
                   null        f7dau_d,
                   null        f30dau_d,
                               fdpu_d,
                               f7dpu_d,
                               f30dpu_d,
                               fdip,
                               f7dip,
                               f30dip,
                   null        fdsuip,
                   null        f7dsuip,
                   null        f30dsuip,
                   null        fdstart_d,
                   null        fdstart_d_num,
                   null        fdsu_d_num,
                   null        fdau_d_cnt,
                   null        fdsupu_d
              from analysis.marketing_fct_dpu_dip_tmp_%(num_begin)s f
            union all
            select fdate,
                   fchannel_id,
                   null        fdsu_d,
                   null        f7dsu_d,
                   null        f30dsu_d,
                   null        fdau_d,
                   null        f7dau_d,
                   null        f30dau_d,
                   null        fdpu_d,
                   null        f7dpu_d,
                   null        f30dpu_d,
                   null        fdip,
                   null        f7dip,
                   null        f30dip,
                               fdsuip,
                               f7dsuip,
                               f30dsuip,
                   null        fdstart_d,
                   null        fdstart_d_num,
                   null        fdsu_d_num,
                   null        fdau_d_cnt,
                               fdsupu_d
              from analysis.marketing_fct_dsuip_tmp_%(num_begin)s g
        ) t;
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """

        insert overwrite table analysis.marketing_channel_dims_fct partition
        (dt = "%(ld_begin)s")
        select t.fdate,
            t.fchannel_id,
            max(t.fdsu_d)                fdsu_d,
            max(t.f7dsu_d)               f7dsu_d,
            max(t.f30dsu_d)              f30dsu_d,
            max(t.fdau_d)                fdau_d,
            max(t.f7dau_d)               f7dau_d,
            max(t.f30dau_d)              f30dau_d,
            max(t.fdpu_d)                fdpu_d,
            max(t.f7dpu_d)               f7dpu_d,
            max(t.f30dpu_d)              f30dpu_d,
            max(t.fdip)                  fdip,
            max(t.f7dip)                 f7dip,
            max(t.f30dip)                f30dip,
            max(t.fdsuip)                fdsuip,
            max(t.f7dsuip)               f7dsuip,
            max(t.f30dsuip)              f30dsuip,
            max(t.fdstart_d)             fdstart_d,
            max(t.fdstart_d_num)         fdstart_d_num,
            max(t.fdsu_d_num)            fdsu_d_num,
            max(t.fdau_d_cnt)            fdau_d_cnt,
            max(t.fdsupu_d)              fdsupu_d
        from analysis.marketing_fct_tmp_%(num_begin)s t
        group by
        t.fdate, t.fchannel_id
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists analysis.marketing_fct_dstart_d_tmp_%(num_begin)s ;
        drop table if exists analysis.marketing_fct_dstart_d_num_tmp_%(num_begin)s ;
        drop table if exists analysis.marketing_fct_dsu_d_tmp_%(num_begin)s ;
        drop table if exists analysis.marketing_fct_dsu_d_num_tmp_%(num_begin)s ;
        drop table if exists analysis.marketing_fct_dau_d_tmp_%(num_begin)s ;
        drop table if exists analysis.marketing_fct_dpu_dip_tmp_%(num_begin)s ;
        drop table if exists analysis.marketing_fct_dsuip_tmp_%(num_begin)s ;
        drop table if exists analysis.marketing_fct_tmp_%(num_begin)s ;
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
    a = new_marketing_fct_data(stat_date)
    a()
