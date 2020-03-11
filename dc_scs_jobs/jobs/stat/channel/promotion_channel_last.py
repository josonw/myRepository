#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class promotion_channel_last(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_channel_fct
        (
        fdate                       date,
        fgamefsk                    bigint,
        fplatformfsk                bigint,
        fversionfsk                 bigint,
        fterminalfsk                bigint,
        fchannel_id                 varchar(64),
        fdsu                        bigint,
        f7dsu                       bigint,
        f14dsu                      bigint,
        f30dsu                      bigint,
        fdau                        bigint,

        f7dau                       bigint,
        f14dau                      bigint,
        f30dau                      bigint,
        fdpu                        bigint,
        f7dpu                       bigint,
        f14dpu                      bigint,
        f30dpu                      bigint,
        fdip                        decimal(20,2),
        f7dip                       decimal(20,2),
        f30dip                      decimal(20,2),
        fth_1dbu                    bigint,
        fth_3dbu                    bigint,
        fth_7dbu                    bigint,
        fth_14dbu                   bigint,
        fth_30dbu                   bigint,
        fdsuip                      decimal(20,2),
        fdsupu                      bigint,
        f7dsuip                     decimal(20,2),
        f30dsuip                    decimal(20,2),
        f7dsupu                     bigint,
        f30dsupu                    bigint,
        frdpu                       bigint,
        f7dfpu                      bigint,
        f30dfpu                     bigint,
        fdfip                       decimal(20,2),
        f7dfip                      decimal(20,2),
        f30dfip                     decimal(20,2),
        f7dayuserloss               bigint,
        f30dayuserloss              bigint,
        freg_gameparty_num          bigint,
        fspucnt                     bigint,
        fpun                        bigint,
        fpucnt                      bigint,
        freg_ruptu                  bigint,
        freg_rupt_cnt               bigint,
        freg_rupt_relieveu          bigint,
        freg_rupt_relieve_cnt       bigint,
        fact_ruptu                  bigint,
        fact_rupt_cnt               bigint,
        fact_rupt_relieveu          bigint,
        fact_rupt_relieve_cnt       bigint,
        frupt_payu                  bigint
        )
        partitioned by (dt date);
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        # self.hq.debug = 1

        hql_list = []

        self.hql_dict['num_30dayago'] = self.hql_dict.get('ld_30dayago').replace('-', '')

        hql = """

        drop table if exists analysis.user_channel_fct_tmp_other_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_other_%(num_begin)s as
        select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fchannel_id,
        max(f7dayuserloss)              f7dayuserloss,
        max(f30dayuserloss)             f30dayuserloss,
        max(freg_gameparty_num)         freg_gameparty_num,
        max(fspucnt)                    fspucnt,
        max(fpun)                       fpun,
        max(fpucnt)                     fpucnt,
        max(freg_ruptu)                 freg_ruptu,
        max(freg_rupt_cnt)              freg_rupt_cnt,
        max(freg_rupt_relieveu)         freg_rupt_relieveu,
        max(freg_rupt_relieve_cnt)      freg_rupt_relieve_cnt,
        max(fact_ruptu)                 fact_ruptu,
        max(fact_rupt_cnt)              fact_rupt_cnt,
        max(fact_rupt_relieveu)         fact_rupt_relieveu,
        max(fact_rupt_relieve_cnt)      fact_rupt_relieve_cnt,
        max(frupt_payu)                 frupt_payu
        from (
            select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fchannel_id,
             f7dayuserloss,
             f30dayuserloss,
            null freg_gameparty_num,
            null fspucnt,
            null fpun,
            null fpucnt,
            null freg_ruptu,
            null freg_rupt_cnt,
            null freg_rupt_relieveu,
            null freg_rupt_relieve_cnt,
            null fact_ruptu,
            null fact_rupt_cnt,
            null fact_rupt_relieveu,
            null fact_rupt_relieve_cnt,
            null frupt_payu
            from analysis.user_channel_fct_day_loss_part_%(num_begin)s a
            union all
            select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fchannel_id,
            null f7dayuserloss,
            null f30dayuserloss,
             freg_gameparty_num,
             fspucnt,
             fpun,
             fpucnt,
            null freg_ruptu,
            null freg_rupt_cnt,
            null freg_rupt_relieveu,
            null freg_rupt_relieve_cnt,
            null fact_ruptu,
            null fact_rupt_cnt,
            null fact_rupt_relieveu,
            null fact_rupt_relieve_cnt,
            null frupt_payu
            from analysis.user_channel_fct_gmprty_cnt_part_%(num_begin)s b
            union all
            select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fchannel_id,
            null f7dayuserloss,
            null f30dayuserloss,
            null freg_gameparty_num,
            null fspucnt,
            null fpun,
            null fpucnt,
             freg_ruptu,
             freg_rupt_cnt,
             freg_rupt_relieveu,
             freg_rupt_relieve_cnt,
             fact_ruptu,
             fact_rupt_cnt,
             fact_rupt_relieveu,
             fact_rupt_relieve_cnt,
             frupt_payu
            from analysis.user_channel_fct_bankrupt_part_%(num_begin)s c
        ) t
        group by fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fchannel_id

        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        insert overwrite table analysis.user_channel_fct partition
        (dt = '%(ld_begin)s')
        select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fchannel_id,
        max(fdsu)                   fdsu,
        max(f7dsu)                  f7dsu,
        max(f14dsu)                 f14dsu,
        max(f30dsu)                 f30dsu,
        max(fdau)                   fdau,
        max(f7dau)                  f7dau,
        max(f14dau)                 f14dau,
        max(f30dau)                 f30dau,
        max(fdpu)                   fdpu,
        max(f7dpu)                  f7dpu,
        max(f14dpu)                 f14dpu,
        max(f30dpu)                 f30dpu,
        max(fdip)                   fdip,
        max(f7dip)                  f7dip,
        max(f30dip)                 f30dip,
        max(fth_1dbu)               fth_1dbu,
        max(fth_3dbu)               fth_3dbu,
        max(fth_7dbu)               fth_7dbu,
        max(fth_14dbu)              fth_14dbu,
        max(fth_30dbu)              fth_30dbu,
        max(fdsuip)                 fdsuip,
        max(fdsupu)                 fdsupu,
        max(f7dsuip)                f7dsuip,
        max(f30dsuip)               f30dsuip,
        max(f7dsupu)                f7dsupu,
        max(f30dsupu)               f30dsupu,
        max(frdpu)                  frdpu,
        max(f7dfpu)                 f7dfpu,
        max(f30dfpu)                f30dfpu,
        max(fdfip)                  fdfip,
        max(f7dfip)                 f7dfip,
        max(f30dfip)                f30dfip,
        max(f7dayuserloss)          f7dayuserloss,
        max(f30dayuserloss)         f30dayuserloss,
        max(freg_gameparty_num)     freg_gameparty_num,
        max(fspucnt)                fspucnt,
        max(fpun)                   fpun,
        max(fpucnt)                 fpucnt,
        max(freg_ruptu)             freg_ruptu,
        max(freg_rupt_cnt)          freg_rupt_cnt,
        max(freg_rupt_relieveu)     freg_rupt_relieveu,
        max(freg_rupt_relieve_cnt)  freg_rupt_relieve_cnt,
        max(fact_ruptu)             fact_ruptu,
        max(fact_rupt_cnt)          fact_rupt_cnt,
        max(fact_rupt_relieveu)     fact_rupt_relieveu,
        max(fact_rupt_relieve_cnt)  fact_rupt_relieve_cnt,
        max(frupt_payu)              frupt_payu
        from (
        select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fchannel_id,
        fdsu,
        f7dsu,
        f14dsu,
        f30dsu,
        fdau,
        f7dau,
        f14dau,
        f30dau,
        fdpu,
        f7dpu,
        f14dpu,
        f30dpu,
        fdip,
        f7dip,
        f30dip,
        fth_1dbu,
        fth_3dbu,
        fth_7dbu,
        fth_14dbu,
        fth_30dbu,
        fdsuip,
        fdsupu,
        f7dsuip,
        f30dsuip,
        f7dsupu,
        f30dsupu,
        frdpu,
        f7dfpu,
        f30dfpu,
        fdfip,
        f7dfip,
        f30dfip,

        null f7dayuserloss,
        null f30dayuserloss,
        null freg_gameparty_num,
        null fspucnt,
        null fpun,
        null fpucnt,
        null freg_ruptu,
        null freg_rupt_cnt,
        null freg_rupt_relieveu,
        null freg_rupt_relieve_cnt,
        null fact_ruptu,
        null fact_rupt_cnt,
        null fact_rupt_relieveu,
        null fact_rupt_relieve_cnt,
        null frupt_payu
        from analysis.user_channel_fct_main_part_%(num_begin)s a
        where fdate = '%(ld_begin)s'
        union all
        select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fchannel_id,
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
        null frdpu,
        null f7dfpu,
        null f30dfpu,
        null fdfip,
        null f7dfip,
        null f30dfip,

        f7dayuserloss,
        f30dayuserloss,
        freg_gameparty_num,
        fspucnt,
        fpun,
        fpucnt,
        freg_ruptu,
        freg_rupt_cnt,
        freg_rupt_relieveu,
        freg_rupt_relieve_cnt,
        fact_ruptu,
        fact_rupt_cnt,
        fact_rupt_relieveu,
        fact_rupt_relieve_cnt,
        frupt_payu
        from analysis.user_channel_fct_tmp_other_%(num_begin)s b
        where fdate = '%(ld_begin)s'
        ) t
        group by fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fchannel_id


        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        drop table if exists analysis.user_channel_fct_day_loss_part_%(num_30dayago)s ;
        drop table if exists analysis.user_channel_fct_gmprty_cnt_part_%(num_30dayago)s ;
        drop table if exists analysis.user_channel_fct_bankrupt_part_%(num_30dayago)s ;
        drop table if exists analysis.user_channel_fct_main_part_%(num_30dayago)s ;
        drop table if exists analysis.user_channel_fct_tmp_other_%(num_30dayago)s ;
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
    a = promotion_channel_last(stat_date)
    a()
