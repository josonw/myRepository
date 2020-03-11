#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

# 已经停止计算
class market_channel_fct_data(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_channel_market_fct
        (
        fdate           date,
        fgamefsk        bigint,
        fplatformfsk    bigint,
        fversionfsk     bigint,
        fterminalfsk    bigint,
        fchannel_id     varchar(64),
        fdsu            bigint,
        f7dsu           bigint,
        f30dsu          bigint,
        fdsu_d          bigint,
        f7dsu_d         bigint,
        f30dsu_d        bigint,
        fdau            bigint,
        f7dau           bigint,
        f30dau          bigint,
        fdau_d          bigint,
        f7dau_d         bigint,
        f30dau_d        bigint,
        fdpu            bigint,
        f7dpu           bigint,
        f30dpu          bigint,
        fdpu_d          bigint,
        f7dpu_d         bigint,
        f30dpu_d        bigint,
        fdip            decimal(20,2),
        f7dip           decimal(20,2),
        f30dip          decimal(20,2),
        frdpu           bigint,
        f7rdpu          bigint,
        f30rdpu         bigint,
        fdfip           decimal(20,2),
        f7dfip          decimal(20,2),
        f30dfip         decimal(20,2),
        fdsuip          decimal(20,2),
        f7dsuip         decimal(20,2),
        f30dsuip        decimal(20,2),
        fmonthregcnt    bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        # hql结尾不要带分号';'
        hql_list = []

        hql = """ set hive.auto.convert.join=false; """
        hql_list.append(hql)

        hql = """

        drop table if exists analysis.market_channel_fct_data_tmp_dsu;
        create table if not exists analysis.market_channel_fct_data_tmp_dsu as
        select '%(ld_begin)s' fdate, a.fbpid, fchannel_id,
                 count(distinct case when a.dt>='%(ld_begin)s'    and a.dt < '%(ld_end)s' then fuid end ) fdsu,
                 count(distinct case when a.dt>=date_add('%(ld_begin)s', -6)  and a.dt < '%(ld_end)s' then fuid end ) f7dsu,
                 count(distinct case when a.dt>=date_add('%(ld_begin)s', -29) and a.dt < '%(ld_end)s' then fuid end ) f30dsu,

                 count(distinct case when a.dt>='%(ld_begin)s'    and a.dt < '%(ld_end)s' then fudid end ) fdsu_d,
                 count(distinct case when a.dt>=date_add('%(ld_begin)s', -6)  and a.dt < '%(ld_end)s' then fudid end ) f7dsu_d,
                 count(distinct case when a.dt>=date_add('%(ld_begin)s', -29) and a.dt < '%(ld_end)s' then fudid end ) f30dsu_d
            from stage.channel_market_reg_mid a
           where a.dt>= date_add('%(ld_begin)s', -29)
             and a.dt < '%(ld_end)s'
        group by a.fbpid,fchannel_id
        """% self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists analysis.market_channel_fct_data_tmp_dau;
        create table if not exists analysis.market_channel_fct_data_tmp_dau as
        select '%(ld_begin)s' fdate, a.fbpid, a.fchannel_id,
                 count(distinct case when fdate>='%(ld_begin)s'    and fdate < '%(ld_end)s' then a.fuid end ) fdau,
                 count(distinct case when fdate>=date_add('%(ld_begin)s', -6)  and fdate < '%(ld_end)s' then a.fuid end ) f7dau,
                 count(distinct case when fdate>=date_add('%(ld_begin)s', -29) and fdate < '%(ld_end)s' then a.fuid end ) f30dau,

                 count(distinct case when fdate>='%(ld_begin)s'    and fdate < '%(ld_end)s' then a.fudid end ) fdau_d,
                 count(distinct case when fdate>=date_add('%(ld_begin)s', -6)  and fdate < '%(ld_end)s' then a.fudid end ) f7dau_d,
                 count(distinct case when fdate>=date_add('%(ld_begin)s', -29) and fdate < '%(ld_end)s' then a.fudid end ) f30dau_d
                from stage.channel_market_active_mid a
               where a.dt>= date_add('%(ld_begin)s', -29)
                 and a.dt < '%(ld_end)s'
        group by a.fbpid, a.fchannel_id
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.market_channel_fct_data_tmp_dip;
        create table if not exists analysis.market_channel_fct_data_tmp_dip as
        select '%(ld_begin)s' fdate, a.fbpid, a.fchannel_id,
                count(distinct case when a.fdate>='%(ld_begin)s' and a.fdate < '%(ld_end)s' then a.fuid end ) fdpu,
                count(distinct case when a.fdate>=date_add('%(ld_begin)s', -6) and a.fdate < '%(ld_end)s' then a.fuid end ) f7dpu,
                count(distinct case when a.fdate>=date_add('%(ld_begin)s', -29) and a.fdate < '%(ld_end)s' then a.fuid end ) f30dpu,
                count(distinct case when a.fdate>='%(ld_begin)s' and a.fdate < '%(ld_end)s' then a.fudid end ) fdpu_d,
                count(distinct case when a.fdate>=date_add('%(ld_begin)s', -6) and a.fdate < '%(ld_end)s' then a.fudid end ) f7dpu_d,
                count(distinct case when a.fdate>=date_add('%(ld_begin)s', -29) and a.fdate < '%(ld_end)s' then a.fudid end ) f30dpu_d,

                cast(round(sum(case when a.fdate>='%(ld_begin)s'               and a.fdate < '%(ld_end)s' then a.fpay_money * a.fpay_rate end ),2) as decimal(20,2) ) fdip,
                cast(round(sum(case when a.fdate>=date_add('%(ld_begin)s',-6)  and a.fdate < '%(ld_end)s' then a.fpay_money * a.fpay_rate end ),2) as decimal(20,2) ) f7dip,
                cast(round(sum(case when a.fdate>=date_add('%(ld_begin)s',-29) and a.fdate < '%(ld_end)s' then a.fpay_money * a.fpay_rate end ),2) as decimal(20,2) ) f30dip

            from stage.channel_market_payment_mid a
           where a.dt >= date_add('%(ld_begin)s', -29)
             and a.dt < '%(ld_end)s'
          group by a.fbpid, a.fchannel_id
          """% self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.market_channel_fct_data_tmp_dfip;
        create table if not exists analysis.market_channel_fct_data_tmp_dfip as
        select '%(ld_begin)s' fdate, a.fbpid, a.fchannel_id,
                 count(distinct case when a.fdate='%(ld_begin)s' then a.fudid end ) frdpu,
                 count(distinct case when a.fdate>=date_add('%(ld_begin)s', -6) and a.fdate< '%(ld_end)s' then a.fudid end ) f7rdpu,
                 count(distinct case when a.fdate>=date_add('%(ld_begin)s', -29) and a.fdate< '%(ld_end)s' then a.fudid end ) f30rdpu,
                 nvl(sum(case when a.fdate='%(ld_begin)s' then dip end ),0) fdfip,
                 nvl(sum(case when a.fdate>=date_add('%(ld_begin)s', -6) and a.fdate<='%(ld_begin)s' then dip end ),0) f7dfip,
                 nvl(sum(case when a.fdate>=date_add('%(ld_begin)s', -29) and a.fdate<='%(ld_begin)s' then dip end ),0) f30dfip
            from ( select fdate, fbpid, fchannel_id,  fuid, min(fudid) fudid, round(sum(fpay_money * fpay_rate),2) dip
                    from stage.channel_market_payment_mid t
                   where t.dt >= date_add('%(ld_begin)s', -29)
                     and t.dt < '%(ld_end)s'
                   group by fdate, fbpid, fchannel_id,  fuid
                  ) a
            join (
                 select ffirst_pay_at fdate, fbpid, fuid from stage.pay_user_mid a
                 where a.dt>=date_add('%(ld_begin)s', -29) and a.dt < '%(ld_end)s'
                  ) c
              on a.fbpid=c.fbpid
             and a.fuid=c.fuid
        group by a.fbpid, a.fchannel_id
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.market_channel_fct_data_tmp_dsuip;
        create table if not exists analysis.market_channel_fct_data_tmp_dsuip as
          select '%(ld_begin)s' fdate, a.fbpid, a.fchannel_id,
                 nvl(sum(case when a.dt>='%(ld_begin)s' and a.dt < '%(ld_end)s' then dip end ),0) fdsuip,
                 nvl(sum(case when a.dt>=date_add('%(ld_begin)s', -6) and a.dt < '%(ld_end)s' then dip end ),0) f7dsuip,
                 nvl(sum(case when a.dt>=date_add('%(ld_begin)s', -29) and a.dt < '%(ld_end)s' then dip end ),0) f30dsuip
            from stage.channel_market_reg_mid a
            join (
                  select fdate, fbpid, fuid, round(sum(fpay_money * fpay_rate),2) dip
                    from stage.channel_market_payment_mid a
                   where a.dt >= date_add('%(ld_begin)s', -29)
                     and a.dt < '%(ld_end)s'
                   group by fdate, fbpid, fuid
                  ) c
              on a.fbpid=c.fbpid
             and a.fuid=c.fuid
             and a.dt >=date_add('%(ld_begin)s', -29) and a.dt < '%(ld_end)s'
             and a.dt = c.fdate
        group by a.fbpid, a.fchannel_id
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.market_channel_fct_data_tmp_monthregcnt;
        create table if not exists analysis.market_channel_fct_data_tmp_monthregcnt as
          select '%(ld_begin)s' fdate, a.fbpid, a.fchannel_id,
                 count(distinct fudid) fmonthregcnt
                from stage.channel_market_reg_mid a
               where a.dt>= '%(ld_month_begin)s'
                 and a.dt < '%(ld_end)s'
            group by a.fbpid, a.fchannel_id
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.market_channel_fct_data_tmp_union;
        create table if not exists analysis.market_channel_fct_data_tmp_union as

        select *
        from (
        select fdate, fbpid, fchannel_id,
         fdsu,
         f7dsu,
         f30dsu,
         fdsu_d,
         f7dsu_d,
         f30dsu_d,
        null fdau,
        null f7dau,
        null f30dau,
        null fdau_d,
        null f7dau_d,
        null f30dau_d,
        null fdpu,
        null f7dpu,
        null f30dpu,
        null fdpu_d,
        null f7dpu_d,
        null f30dpu_d,
        null fdip,
        null f7dip,
        null f30dip,
        null frdpu,
        null f7rdpu,
        null f30rdpu,
        null fdfip,
        null f7dfip,
        null f30dfip,
        null fdsuip,
        null f7dsuip,
        null f30dsuip,
        null fmonthregcnt
        from analysis.market_channel_fct_data_tmp_dsu a
        union all
        select fdate, fbpid, fchannel_id,
        null fdsu,
        null f7dsu,
        null f30dsu,
        null fdsu_d,
        null f7dsu_d,
        null f30dsu_d,
         fdau,
         f7dau,
         f30dau,
         fdau_d,
         f7dau_d,
         f30dau_d,
        null fdpu,
        null f7dpu,
        null f30dpu,
        null fdpu_d,
        null f7dpu_d,
        null f30dpu_d,
        null fdip,
        null f7dip,
        null f30dip,
        null frdpu,
        null f7rdpu,
        null f30rdpu,
        null fdfip,
        null f7dfip,
        null f30dfip,
        null fdsuip,
        null f7dsuip,
        null f30dsuip,
        null fmonthregcnt
        from analysis.market_channel_fct_data_tmp_dau b
        union all
        select fdate, fbpid, fchannel_id,
        null fdsu,
        null f7dsu,
        null f30dsu,
        null fdsu_d,
        null f7dsu_d,
        null f30dsu_d,
        null fdau,
        null f7dau,
        null f30dau,
        null fdau_d,
        null f7dau_d,
        null f30dau_d,
         fdpu,
         f7dpu,
         f30dpu,
         fdpu_d,
         f7dpu_d,
         f30dpu_d,
         fdip,
         f7dip,
         f30dip,
        null frdpu,
        null f7rdpu,
        null f30rdpu,
        null fdfip,
        null f7dfip,
        null f30dfip,
        null fdsuip,
        null f7dsuip,
        null f30dsuip,
        null fmonthregcnt
        from analysis.market_channel_fct_data_tmp_dip c
        union all
        select fdate, fbpid, fchannel_id,
        null fdsu,
        null f7dsu,
        null f30dsu,
        null fdsu_d,
        null f7dsu_d,
        null f30dsu_d,
        null fdau,
        null f7dau,
        null f30dau,
        null fdau_d,
        null f7dau_d,
        null f30dau_d,
        null fdpu,
        null f7dpu,
        null f30dpu,
        null fdpu_d,
        null f7dpu_d,
        null f30dpu_d,
        null fdip,
        null f7dip,
        null f30dip,
         frdpu,
         f7rdpu,
         f30rdpu,
         fdfip,
         f7dfip,
         f30dfip,
        null fdsuip,
        null f7dsuip,
        null f30dsuip,
        null fmonthregcnt
        from analysis.market_channel_fct_data_tmp_dfip d
        union all
        select fdate, fbpid, fchannel_id,
        null fdsu,
        null f7dsu,
        null f30dsu,
        null fdsu_d,
        null f7dsu_d,
        null f30dsu_d,
        null fdau,
        null f7dau,
        null f30dau,
        null fdau_d,
        null f7dau_d,
        null f30dau_d,
        null fdpu,
        null f7dpu,
        null f30dpu,
        null fdpu_d,
        null f7dpu_d,
        null f30dpu_d,
        null fdip,
        null f7dip,
        null f30dip,
        null frdpu,
        null f7rdpu,
        null f30rdpu,
        null fdfip,
        null f7dfip,
        null f30dfip,
         fdsuip,
         f7dsuip,
         f30dsuip,
        null fmonthregcnt
        from analysis.market_channel_fct_data_tmp_dsuip e
        union all
        select fdate, fbpid, fchannel_id,
        null fdsu,
        null f7dsu,
        null f30dsu,
        null fdsu_d,
        null f7dsu_d,
        null f30dsu_d,
        null fdau,
        null f7dau,
        null f30dau,
        null fdau_d,
        null f7dau_d,
        null f30dau_d,
        null fdpu,
        null f7dpu,
        null f30dpu,
        null fdpu_d,
        null f7dpu_d,
        null f30dpu_d,
        null fdip,
        null f7dip,
        null f30dip,
        null frdpu,
        null f7rdpu,
        null f30rdpu,
        null fdfip,
        null f7dfip,
        null f30dfip,
        null fdsuip,
        null f7dsuip,
        null f30dsuip,
         fmonthregcnt
        from analysis.market_channel_fct_data_tmp_monthregcnt f
        ) t
        """ % self.hql_dict
        hql_list.append( hql )


        hql = """


        insert overwrite table analysis.user_channel_market_fct partition
        (dt = "%(ld_begin)s")
        select
        a.fdate,
        b.fgamefsk,
        b.fplatformfsk,
        b.fversion_old fversionfsk,
        b.fterminalfsk,
        a.fchannel_id,
        max(a.fdsu)             fdsu,
        max(a.f7dsu)            f7dsu,
        max(a.f30dsu)           f30dsu,
        max(a.fdsu_d)           fdsu_d,
        max(a.f7dsu_d)          f7dsu_d,
        max(a.f30dsu_d)         f30dsu_d,
        max(a.fdau)             fdau,
        max(a.f7dau)            f7dau,
        max(a.f30dau)           f30dau,
        max(a.fdau_d)           fdau_d,
        max(a.f7dau_d)          f7dau_d,
        max(a.f30dau_d)         f30dau_d,
        max(a.fdpu)             fdpu,
        max(a.f7dpu)            f7dpu,
        max(a.f30dpu)           f30dpu,
        max(a.fdpu_d)           fdpu_d,
        max(a.f7dpu_d)          f7dpu_d,
        max(a.f30dpu_d)         f30dpu_d,
        max(a.fdip)             fdip,
        max(a.f7dip)            f7dip,
        max(a.f30dip)           f30dip,
        max(a.frdpu)            frdpu,
        max(a.f7rdpu)           f7rdpu,
        max(a.f30rdpu)          f30rdpu,
        max(a.fdfip)            fdfip,
        max(a.f7dfip)           f7dfip,
        max(a.f30dfip)          f30dfip,
        max(a.fdsuip)           fdsuip,
        max(a.f7dsuip)          f7dsuip,
        max(a.f30dsuip)         f30dsuip,
        max(a.fmonthregcnt)     fmonthregcnt
        from analysis.market_channel_fct_data_tmp_union a
        join dim.bpid_map b
        on a.fbpid = b.fbpid
        group by a.fdate,
                 b.fgamefsk,
                 b.fplatformfsk,
                 b.fversion_old,
                 b.fterminalfsk,
                 a.fchannel_id


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
    a = market_channel_fct_data(stat_date)
    a()
