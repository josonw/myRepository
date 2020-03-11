#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class promotion_channel_fct_data(BaseStat):

    def create_tab(self):
        pass

    def stat(self):
        # self.hq.debug = 1

        hql_list = []

        hql = """
        drop table if exists analysis.user_channel_fct_tmp_dsu_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_dsu_%(num_begin)s as
        select "%(ld_begin)s" fdate, fgamefsk,fplatformfsk,fversion_old fversionfsk, fterminalfsk, fpkg_channel_id fchannel_id,
             count(distinct case when a.dt>="%(ld_begin)s" and a.dt < '%(ld_end)s' then fuid end ) fdsu,
             count(distinct case when a.dt>=date_add("%(ld_begin)s", -6) and a.dt < '%(ld_end)s' then fuid end ) f7dsu,
             count(distinct case when a.dt>=date_add("%(ld_begin)s", -14) and a.dt < '%(ld_end)s' then fuid end ) f14dsu,
             count(distinct case when a.dt>=date_add("%(ld_begin)s", -29) and a.dt < '%(ld_end)s' then fuid end ) f30dsu
        from stage.channel_market_reg_mid a
        join analysis.dc_channel_package b
          on a.fnow_channel_id = b.fpkg_id
        join dim.bpid_map d
          on a.fbpid=d.fbpid
         and a.dt>= date_add("%(ld_begin)s", -29)
         and a.dt < '%(ld_end)s'
        group By fgamefsk,fplatformfsk,fversion_old,fterminalfsk,fpkg_channel_id;

        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.user_channel_fct_tmp_dau_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_dau_%(num_begin)s as
        select "%(ld_begin)s" fdate, fgamefsk,fplatformfsk,fversion_old fversionfsk, fterminalfsk, fpkg_channel_id fchannel_id,
                 count(distinct case when a.fdate>="%(ld_begin)s" and a.fdate < '%(ld_end)s' then fuid end ) fdau,
                 count(distinct case when a.fdate>=date_add("%(ld_begin)s", -6) and a.fdate < '%(ld_end)s' then fuid end ) f7dau,
                 count(distinct case when a.fdate>=date_add("%(ld_begin)s", -14) and a.fdate < '%(ld_end)s' then fuid end ) f14dau,
                 count(distinct case when a.fdate>=date_add("%(ld_begin)s", -29) and a.fdate < '%(ld_end)s' then fuid end ) f30dau
            from stage.channel_market_active_mid a
            join analysis.dc_channel_package b
              on a.fchannel_id = b.fpkg_id
            join dim.bpid_map d
              on a.fbpid=d.fbpid
            where a.dt>= date_add("%(ld_begin)s", -29)
              and a.dt < '%(ld_end)s'
             group By fgamefsk,fplatformfsk,fversion_old,fterminalfsk,fpkg_channel_id;
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.user_channel_fct_tmp_dpu_dip_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_dpu_dip_%(num_begin)s as
        select "%(ld_begin)s" fdate, fgamefsk,fplatformfsk,fversion_old fversionfsk,fterminalfsk, b.fpkg_channel_id fchannel_id,
                 count(distinct case when a.fdate>="%(ld_begin)s" and a.fdate < '%(ld_end)s' then a.fuid end ) fdpu,
                 count(distinct case when a.fdate>=date_add("%(ld_begin)s", -6) and a.fdate < '%(ld_end)s' then a.fuid end ) f7dpu,
                 count(distinct case when a.fdate>=date_add("%(ld_begin)s", -14) and a.fdate < '%(ld_end)s' then a.fuid end ) f14dpu,
                 count(distinct case when a.fdate>=date_add("%(ld_begin)s", -29) and a.fdate < '%(ld_end)s' then a.fuid end ) f30dpu,

                cast(round(sum(case when a.fdate>='%(ld_begin)s'               and a.fdate < '%(ld_end)s' then a.fpay_money * a.fpay_rate end ),2) as decimal(20,2) ) fdip,
                cast(round(sum(case when a.fdate>=date_add('%(ld_begin)s',-6)  and a.fdate < '%(ld_end)s' then a.fpay_money * a.fpay_rate end ),2) as decimal(20,2) ) f7dip,
                cast(round(sum(case when a.fdate>=date_add('%(ld_begin)s',-29) and a.fdate < '%(ld_end)s' then a.fpay_money * a.fpay_rate end ),2) as decimal(20,2) ) f30dip

            from stage.channel_market_payment_mid a
            join analysis.dc_channel_package b
              on a.fchannel_id = b.fpkg_id
            join dim.bpid_map d
              on a.fbpid=d.fbpid
           where a.dt>= date_add("%(ld_begin)s", -29)
             and a.dt < '%(ld_end)s'
          group By fgamefsk,fplatformfsk,fversion_old,fterminalfsk, b.fpkg_channel_id;

          """% self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists analysis.user_channel_fct_tmp_th_dbu_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_th_dbu_%(num_begin)s as
          select "%(ld_begin)s" fdate, fgamefsk,fplatformfsk,fversion_old fversionfsk,fterminalfsk, fpkg_channel_id fchannel_id,
                 count(distinct case when a.fdate = date_add("%(ld_begin)s",-1) then fuid end ) fth_1dbu,
                 count(distinct case when a.fdate = date_add("%(ld_begin)s", -3) then fuid end ) fth_3dbu,
                 count(distinct case when a.fdate = date_add("%(ld_begin)s", -7) then fuid end ) fth_7dbu,
                 count(distinct case when a.fdate = date_add("%(ld_begin)s", -14) then fuid end ) fth_14dbu,
                 count(distinct case when a.fdate = date_add("%(ld_begin)s", -30) then fuid end ) fth_30dbu
            from (select a.fbpid, fnow_channel_id, a.fuid, a.dt fdate
                    from stage.channel_market_reg_mid a
                    join stage.active_user_mid c
                      on a.fbpid=c.fbpid
                     and a.fuid=c.fuid
                   where a.dt >= date_add("%(ld_begin)s", -30)
                     and a.dt <  '%(ld_end)s'
                     and c.dt = "%(ld_begin)s"
                 ) a
            join analysis.dc_channel_package b
              on a.fnow_channel_id = b.fpkg_id
            join dim.bpid_map d
              on a.fbpid=d.fbpid
          group By fgamefsk,fplatformfsk,fversion_old,fterminalfsk, fpkg_channel_id;
          """% self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists analysis.user_channel_fct_tmp_dfpu_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_dfpu_%(num_begin)s as
          select "%(ld_begin)s" fdate, fgamefsk,fplatformfsk,fversion_old fversionfsk,fterminalfsk, fpkg_channel_id fchannel_id,
                 count(distinct case when a.fdate="%(ld_begin)s" then a.fuid end ) frdpu,
                 count(distinct case when a.fdate>=date_add("%(ld_begin)s", -6) and a.fdate< '%(ld_end)s' then a.fuid end ) f7dfpu,
                 count(distinct case when a.fdate>=date_add("%(ld_begin)s", -29) and a.fdate< '%(ld_end)s' then a.fuid end ) f30dfpu,
                 nvl(sum(case when a.fdate="%(ld_begin)s" then DIP end ),0) fdfip,
                 nvl(sum(case when a.fdate>=date_add("%(ld_begin)s", -6) and a.fdate<="%(ld_begin)s" then DIP end ),0) f7dfip,
                 nvl(sum(case when a.fdate>=date_add("%(ld_begin)s", -29) and a.fdate<="%(ld_begin)s" then DIP end ),0) f30dfip
            from (
                  select a.fdate, a.fbpid, a.fchannel_id, a.fuid, round(sum(fpay_money * fpay_rate),2) dip
                    from stage.channel_market_payment_mid a
                    where a.dt >= date_add("%(ld_begin)s", -29) and a.dt< '%(ld_end)s'
                    group by a.fdate, a.fbpid, a.fchannel_id, a.fuid ) a
            join analysis.dc_channel_package b
              on a.fchannel_id = b.fpkg_id
            join (
                 select ffirst_pay_at fdate, fbpid, fuid from stage.pay_user_mid
                 where ffirst_pay_at>=date_add("%(ld_begin)s", -29) and ffirst_pay_at < '%(ld_end)s'
                 group by ffirst_pay_at, fbpid, fuid
                  ) c
              on a.fbpid=c.fbpid
             and a.fuid=c.fuid
             and a.fdate >=date_add("%(ld_begin)s", -29) and a.fdate < '%(ld_end)s'
             and a.fdate = c.fdate
            join dim.bpid_map d
              on a.fbpid=d.fbpid
        group By fgamefsk,fplatformfsk,fversion_old,fterminalfsk, fpkg_channel_id;
        """% self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists analysis.user_channel_fct_tmp_dsupu_dsuip_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_dsupu_dsuip_%(num_begin)s as
        select "%(ld_begin)s" fdate,
                  fgamefsk,
                  fplatformfsk,
                  fversion_old fversionfsk,
                  fterminalfsk,
                  fpkg_channel_id fchannel_id,
                  sum(fdsupu) fdsupu,
                  sum(fdsuip) fdsuip,
                  sum(f7dsupu) f7dsupu,
                  sum(f7dsuip) f7dsuip,
                  sum(f30dsupu) f30dsupu,
                  sum(f30dsuip) f30dsuip
             from (select
                    a.fbpid,
                    c.fpkg_channel_id,
                    count(distinct case
                            when b.dt >= "%(ld_begin)s" and b.dt < '%(ld_end)s' and
                                 a.fdate >= "%(ld_begin)s" and a.fdate < '%(ld_end)s' then
                             a.fuid
                          end) fdsupu,
                    sum(case
                          when b.dt >= "%(ld_begin)s" and b.dt < '%(ld_end)s' and
                               a.fdate >= "%(ld_begin)s" and a.fdate < '%(ld_end)s' then
                           nvl(round(fpay_money * fpay_rate, 2), 0)
                        end) fdsuip,
                    count(distinct case
                            when b.dt >= date_add("%(ld_begin)s",-6) and b.dt < '%(ld_end)s' and
                                 a.fdate >= date_add("%(ld_begin)s",-6) and a.fdate < '%(ld_end)s' then
                             a.fuid
                          end) f7dsupu,
                    sum(case
                          when b.dt >= "%(ld_begin)s" and b.dt < '%(ld_end)s' and
                               a.fdate >= date_add("%(ld_begin)s",-6) and a.fdate < '%(ld_end)s' then
                           nvl(round(fpay_money * fpay_rate, 2), 0)
                        end) f7dsuip,
                    count(distinct a.fuid) f30dsupu,
                    sum(nvl(round(fpay_money * fpay_rate, 2), 0)) f30dsuip

                     from stage.channel_market_payment_mid a
                     join stage.channel_market_reg_mid b
                       on a.fbpid = b.fbpid
                      and a.fuid = b.fuid
                     join analysis.dc_channel_package c
                       on a.fchannel_id = c.fpkg_id
                    where a.dt >= date_add("%(ld_begin)s", -29)
                      and a.dt < '%(ld_end)s'
                      and b.dt >= date_add("%(ld_begin)s", -29)
                      and b.dt < '%(ld_end)s'
                    group by a.fbpid, c.fpkg_channel_id) ta
             join dim.bpid_map tb
               on ta.fbpid = tb.fbpid
            group By fgamefsk,
                     fplatformfsk,
                     fversion_old,
                     fterminalfsk,
                     fpkg_channel_id;

        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists analysis.user_channel_fct_main_part_union_tmp_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_main_part_union_tmp_%(num_begin)s as
        select * from
        (
         select
         fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fchannel_id,
         fdsu,
         f7dsu,
         f14dsu,
         f30dsu,
        null fdau,
        null f7dau,
        null f14dau,
        null f30dau,
        null fdpu,
        null f7dpu,
        null f14dpu,
        null f30dpu,
        null fdip,
        null f7dip,
        null f30dip,
        null fth_1dbu,
        null fth_3dbu,
        null fth_7dbu,
        null fth_14dbu,
        null fth_30dbu,
        null fdsuip,
        null fdsupu,
        null f7dsuip,
        null f30dsuip,
        null f7dsupu,
        null f30dsupu,
        null frdpu,
        null f7dfpu,
        null f30dfpu,
        null fdfip,
        null f7dfip,
        null f30dfip
        from analysis.user_channel_fct_tmp_dsu_%(num_begin)s a
        union all
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fchannel_id,
        null fdsu,
        null f7dsu,
        null f14dsu,
        null f30dsu,
         fdau,
         f7dau,
         f14dau,
         f30dau,
        null fdpu,
        null f7dpu,
        null f14dpu,
        null f30dpu,
        null fdip,
        null f7dip,
        null f30dip,
        null fth_1dbu,
        null fth_3dbu,
        null fth_7dbu,
        null fth_14dbu,
        null fth_30dbu,
        null fdsuip,
        null fdsupu,
        null f7dsuip,
        null f30dsuip,
        null f7dsupu,
        null f30dsupu,
        null frdpu,
        null f7dfpu,
        null f30dfpu,
        null fdfip,
        null f7dfip,
        null f30dfip
        from analysis.user_channel_fct_tmp_dau_%(num_begin)s b
        union all
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fchannel_id,
        null fdsu,
        null f7dsu,
        null f14dsu,
        null f30dsu,
        null fdau,
        null f7dau,
        null f14dau,
        null f30dau,
         fdpu,
         f7dpu,
         f14dpu,
         f30dpu,
         fdip,
         f7dip,
         f30dip,
        null fth_1dbu,
        null fth_3dbu,
        null fth_7dbu,
        null fth_14dbu,
        null fth_30dbu,
        null fdsuip,
        null fdsupu,
        null f7dsuip,
        null f30dsuip,
        null f7dsupu,
        null f30dsupu,
        null frdpu,
        null f7dfpu,
        null f30dfpu,
        null fdfip,
        null f7dfip,
        null f30dfip
        from analysis.user_channel_fct_tmp_dpu_dip_%(num_begin)s c
        union all
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fchannel_id,
        null fdsu,
        null f7dsu,
        null f14dsu,
        null f30dsu,
        null fdau,
        null f7dau,
        null f14dau,
        null f30dau,
        null fdpu,
        null f7dpu,
        null f14dpu,
        null f30dpu,
        null fdip,
        null f7dip,
        null f30dip,
         fth_1dbu,
         fth_3dbu,
         fth_7dbu,
         fth_14dbu,
         fth_30dbu,
        null fdsuip,
        null fdsupu,
        null f7dsuip,
        null f30dsuip,
        null f7dsupu,
        null f30dsupu,
        null frdpu,
        null f7dfpu,
        null f30dfpu,
        null fdfip,
        null f7dfip,
        null f30dfip
        from analysis.user_channel_fct_tmp_th_dbu_%(num_begin)s d
        union all
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fchannel_id,
        null fdsu,
        null f7dsu,
        null f14dsu,
        null f30dsu,
        null fdau,
        null f7dau,
        null f14dau,
        null f30dau,
        null fdpu,
        null f7dpu,
        null f14dpu,
        null f30dpu,
        null fdip,
        null f7dip,
        null f30dip,
        null fth_1dbu,
        null fth_3dbu,
        null fth_7dbu,
        null fth_14dbu,
        null fth_30dbu,
        null fdsuip,
        null fdsupu,
        null f7dsuip,
        null f30dsuip,
        null f7dsupu,
        null f30dsupu,
         frdpu,
         f7dfpu,
         f30dfpu,
         fdfip,
         f7dfip,
         f30dfip
         from analysis.user_channel_fct_tmp_dfpu_%(num_begin)s e
        union all
         select
         fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fchannel_id,
        null fdsu,
        null f7dsu,
        null f14dsu,
        null f30dsu,
        null fdau,
        null f7dau,
        null f14dau,
        null f30dau,
        null fdpu,
        null f7dpu,
        null f14dpu,
        null f30dpu,
        null fdip,
        null f7dip,
        null f30dip,
        null fth_1dbu,
        null fth_3dbu,
        null fth_7dbu,
        null fth_14dbu,
        null fth_30dbu,
         fdsuip,
         fdsupu,
         f7dsuip,
         f30dsuip,
         f7dsupu,
         f30dsupu,
        null frdpu,
        null f7dfpu,
        null f30dfpu,
        null fdfip,
        null f7dfip,
        null f30dfip
        from analysis.user_channel_fct_tmp_dsupu_dsuip_%(num_begin)s f

        ) t """% self.hql_dict
        hql_list.append( hql )


        hql = """
        -- table analysis.user_channel_fct_main_part 为了生成依赖关系
        drop table if exists analysis.user_channel_fct_main_part_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_main_part_%(num_begin)s as
        select
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fchannel_id,
        max(fdsu) fdsu,
        max(f7dsu) f7dsu,
        max(f14dsu) f14dsu,
        max(f30dsu) f30dsu,
        max(fdau) fdau,
        max(f7dau) f7dau,
        max(f14dau) f14dau,
        max(f30dau) f30dau,
        max(fdpu) fdpu,
        max(f7dpu) f7dpu,
        max(f14dpu) f14dpu,
        max(f30dpu) f30dpu,
        max(fdip) fdip,
        max(f7dip) f7dip,
        max(f30dip) f30dip,
        max(fth_1dbu) fth_1dbu,
        max(fth_3dbu) fth_3dbu,
        max(fth_7dbu) fth_7dbu,
        max(fth_14dbu) fth_14dbu,
        max(fth_30dbu) fth_30dbu,
        max(fdsuip) fdsuip,
        max(fdsupu) fdsupu,
        max(f7dsuip) f7dsuip,
        max(f30dsuip) f30dsuip,
        max(f7dsupu) f7dsupu,
        max(f30dsupu) f30dsupu,
        max(frdpu) frdpu,
        max(f7dfpu) f7dfpu,
        max(f30dfpu) f30dfpu,
        max(fdfip) fdfip,
        max(f7dfip) f7dfip,
        max(f30dfip) f30dfip
        from analysis.user_channel_fct_main_part_union_tmp_%(num_begin)s t
        group by
        fdate,
        fgamefsk,
        fplatformfsk,
        fversionfsk,
        fterminalfsk,
        fchannel_id """ % self.hql_dict
        hql_list.append( hql )



        hql = """
        drop table if exists analysis.user_channel_fct_tmp_dsu_%(num_begin)s;
        drop table if exists analysis.user_channel_fct_tmp_dau_%(num_begin)s;
        drop table if exists analysis.user_channel_fct_tmp_dpu_dip_%(num_begin)s;
        drop table if exists analysis.user_channel_fct_tmp_th_dbu_%(num_begin)s;
        drop table if exists analysis.user_channel_fct_tmp_dfpu_%(num_begin)s;
        drop table if exists analysis.user_channel_fct_tmp_dsupu_dsuip_%(num_begin)s;
        drop table if exists analysis.user_channel_fct_main_part_union_tmp_%(num_begin)s;
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
    a = promotion_channel_fct_data(stat_date)
    a()
