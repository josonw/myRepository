#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class promotion_channel_bankrupt(BaseStat):

    def create_tab(self):
        pass


    def stat(self):
        # self.hq.debug = 1

        hql_list = []

        hql = """
        drop table if exists stage.channel_bankrupt_relieve_tmp_%(num_begin)s;
        create table if not exists stage.channel_bankrupt_relieve_tmp_%(num_begin)s as
        select fbpid, fuid, fcnt
         from stage.user_bankrupt_relieve_stg a
        where a.dt = '%(ld_begin)s'
        """% self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists analysis.user_channel_fct_tmp_reg_ruptu_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_reg_ruptu_%(num_begin)s as
        select  '%(ld_begin)s' fdate, fgamefsk,fplatformfsk,fversion_old fversionfsk,fterminalfsk, fpkg_channel_id fchannel_id,
                 count(distinct a.fuid ) freg_ruptu,                  --新增破产用户数
                 count(1) freg_rupt_cnt                               --新增破产次数
            from (select a.fbpid, fnow_channel_id, b.fuid
                    from stage.channel_market_reg_mid a
                    join stage.user_bankrupt_stg b
                      on a.fbpid=b.fbpid
                     and a.fuid=b.fuid
                   where a.dt = '%(ld_begin)s'
                     and b.dt = '%(ld_begin)s'
                 ) a
            join analysis.dc_channel_package b
              on a.fnow_channel_id = b.fpkg_id
            join dim.bpid_map d
              on a.fbpid=d.fbpid
          group By fgamefsk,fplatformfsk,fversion_old,fterminalfsk, fpkg_channel_id
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.user_channel_fct_tmp_reg_relieve_cnt_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_reg_relieve_cnt_%(num_begin)s as
        select  '%(ld_begin)s' fdate, fgamefsk,fplatformfsk,fversion_old fversionfsk,fterminalfsk, fpkg_channel_id fchannel_id,
                 count(distinct fuid ) freg_rupt_relieveu,          --新增破产接收救济用户数
                 count(1) freg_rupt_relieve_cnt                     --新增破产接收救济次数
            from (select a.fbpid, fnow_channel_id, b.fuid
                    from stage.channel_market_reg_mid a
                    join stage.channel_bankrupt_relieve_tmp_%(num_begin)s b
                      on a.fbpid=b.fbpid
                     and a.fuid=b.fuid
                   where a.dt = '%(ld_begin)s'
                 ) a
            join analysis.dc_channel_package b
              on a.fnow_channel_id = b.fpkg_id
            join dim.bpid_map d
              on a.fbpid=d.fbpid
          group By fgamefsk,fplatformfsk,fversion_old,fterminalfsk, fpkg_channel_id
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.user_channel_fct_tmp_act_ruptu_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_act_ruptu_%(num_begin)s as
        select  '%(ld_begin)s' fdate, fgamefsk,fplatformfsk,fversion_old fversionfsk,fterminalfsk, fpkg_channel_id fchannel_id,
                 count(distinct a.fuid ) fact_ruptu,                 --活跃破产用户数
                 count(1) fact_rupt_cnt                              --活跃破产次数
            from (select a.fbpid, fchannel_id, b.fuid
                    from stage.channel_market_active_mid a
                    join stage.user_bankrupt_stg b
                      on a.fbpid=b.fbpid
                     and a.fuid=b.fuid
                   where a.dt = '%(ld_begin)s'
                     and b.dt = '%(ld_begin)s'
                 ) a
            join analysis.dc_channel_package b
              on a.fchannel_id = b.fpkg_id
            join dim.bpid_map d
              on a.fbpid=d.fbpid
          group By fgamefsk,fplatformfsk,fversion_old,fterminalfsk, fpkg_channel_id
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.user_channel_fct_tmp_act_relieve_cnt_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_act_relieve_cnt_%(num_begin)s as
        select  '%(ld_begin)s' fdate, fgamefsk,fplatformfsk,fversion_old fversionfsk,fterminalfsk, fpkg_channel_id fchannel_id,
                 count(distinct fuid) fact_rupt_relieveu,               --活跃破产接收救济用户数
                 count(1) fact_rupt_relieve_cnt                         --活跃破产接收救济次数
            from (select a.fbpid, fchannel_id, b.fuid
                    from stage.channel_market_active_mid a
                    join stage.channel_bankrupt_relieve_tmp_%(num_begin)s b
                      on a.fbpid=b.fbpid
                     and a.fuid=b.fuid
                   where a.dt = '%(ld_begin)s'
                 ) a
            join analysis.dc_channel_package b
              on a.fchannel_id = b.fpkg_id
            join dim.bpid_map d
              on a.fbpid=d.fbpid
          group By fgamefsk,fplatformfsk,fversion_old,fterminalfsk, fpkg_channel_id
        """% self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.user_channel_fct_tmp_pay_rupt_payu_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_pay_rupt_payu_%(num_begin)s as
        select  '%(ld_begin)s' fdate, fgamefsk,fplatformfsk,fversion_old fversionfsk,fterminalfsk, fpkg_channel_id fchannel_id,
                 count(distinct a.fuid ) frupt_payu                      --破产付费用户数
            from (select a.fbpid, fchannel_id, a.fuid
                    from stage.channel_market_payment_mid a
                    join stage.user_bankrupt_stg b
                      on a.fbpid=b.fbpid
                     and a.fuid=b.fuid
                   where a.dt = '%(ld_begin)s'
                     and b.dt = '%(ld_begin)s'
                 ) a
            join analysis.dc_channel_package b
              on a.fchannel_id = b.fpkg_id
            join dim.bpid_map d
              on a.fbpid=d.fbpid
          group By fgamefsk,fplatformfsk,fversion_old,fterminalfsk, fpkg_channel_id

        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- table analysis.user_channel_fct_bankrupt_part 为了生成依赖关系
        drop table if exists analysis.user_channel_fct_bankrupt_part_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_bankrupt_part_%(num_begin)s as
        select fdate, fgamefsk,fplatformfsk, fversionfsk, fterminalfsk,  fchannel_id,
        max(freg_ruptu)             freg_ruptu,
        max(freg_rupt_cnt)          freg_rupt_cnt,
        max(freg_rupt_relieveu)     freg_rupt_relieveu,
        max(freg_rupt_relieve_cnt)  freg_rupt_relieve_cnt,
        max(fact_ruptu)             fact_ruptu,
        max(fact_rupt_cnt)          fact_rupt_cnt,
        max(fact_rupt_relieveu)     fact_rupt_relieveu,
        max(fact_rupt_relieve_cnt)  fact_rupt_relieve_cnt,
        max(frupt_payu)             frupt_payu
        from (
            select
            fdate, fgamefsk,fplatformfsk, fversionfsk, fterminalfsk,  fchannel_id,
            freg_ruptu,
            freg_rupt_cnt,
            null freg_rupt_relieveu,
            null freg_rupt_relieve_cnt,
            null fact_ruptu,
            null fact_rupt_cnt,
            null fact_rupt_relieveu,
            null fact_rupt_relieve_cnt,
            null frupt_payu
            from analysis.user_channel_fct_tmp_reg_ruptu_%(num_begin)s a
            union all
            select
            fdate, fgamefsk,fplatformfsk, fversionfsk, fterminalfsk,  fchannel_id,
            null freg_ruptu,
            null freg_rupt_cnt,
             freg_rupt_relieveu,
             freg_rupt_relieve_cnt,
            null fact_ruptu,
            null fact_rupt_cnt,
            null fact_rupt_relieveu,
            null fact_rupt_relieve_cnt,
            null frupt_payu
            from analysis.user_channel_fct_tmp_reg_relieve_cnt_%(num_begin)s b
            union all
            select
            fdate, fgamefsk,fplatformfsk, fversionfsk, fterminalfsk,  fchannel_id,
            null freg_ruptu,
            null freg_rupt_cnt,
            null freg_rupt_relieveu,
            null freg_rupt_relieve_cnt,
             fact_ruptu,
             fact_rupt_cnt,
            null fact_rupt_relieveu,
            null fact_rupt_relieve_cnt,
            null frupt_payu
            from analysis.user_channel_fct_tmp_act_ruptu_%(num_begin)s c
            union all
            select
            fdate, fgamefsk,fplatformfsk, fversionfsk, fterminalfsk,  fchannel_id,
            null freg_ruptu,
            null freg_rupt_cnt,
            null freg_rupt_relieveu,
            null freg_rupt_relieve_cnt,
            null fact_ruptu,
            null fact_rupt_cnt,
             fact_rupt_relieveu,
             fact_rupt_relieve_cnt,
            null frupt_payu
            from analysis.user_channel_fct_tmp_act_relieve_cnt_%(num_begin)s d
            union all
            select
            fdate, fgamefsk,fplatformfsk, fversionfsk, fterminalfsk,  fchannel_id,
            null freg_ruptu,
            null freg_rupt_cnt,
            null freg_rupt_relieveu,
            null freg_rupt_relieve_cnt,
            null fact_ruptu,
            null fact_rupt_cnt,
            null fact_rupt_relieveu,
            null fact_rupt_relieve_cnt,
             frupt_payu
            from analysis.user_channel_fct_tmp_pay_rupt_payu_%(num_begin)s e
        ) t
        group by fdate, fgamefsk,fplatformfsk, fversionfsk, fterminalfsk,  fchannel_id

        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists stage.channel_bankrupt_relieve_tmp_%(num_begin)s;

        drop table if exists analysis.user_channel_fct_tmp_reg_ruptu_%(num_begin)s;
        drop table if exists analysis.user_channel_fct_tmp_reg_relieve_cnt_%(num_begin)s;
        drop table if exists analysis.user_channel_fct_tmp_act_ruptu_%(num_begin)s;
        drop table if exists analysis.user_channel_fct_tmp_act_relieve_cnt_%(num_begin)s;
        drop table if exists analysis.user_channel_fct_tmp_pay_rupt_payu_%(num_begin)s;

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
    a = promotion_channel_bankrupt(stat_date)
    a()
