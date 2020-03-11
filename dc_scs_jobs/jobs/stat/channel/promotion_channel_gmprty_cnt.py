#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class promotion_channel_gmprty_cnt(BaseStat):

    def create_tab(self):
        pass


    def stat(self):
        # self.hq.debug = 1

        hql_list = []

        hql = """
        -- 今天注册玩牌的用户数
        -- 依赖表user_gameparty_info_mid 先完成

        drop table if exists analysis.user_channel_fct_tmp_reg_gameparty_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_reg_gameparty_%(num_begin)s as
        select '%(ld_begin)s' fdate,
               fgamefsk,
               fplatformfsk,
               fversion_old fversionfsk,
               fterminalfsk,
               d.fpkg_channel_id fchannel_id,
               count(distinct a.fuid) freg_gameparty_num,
               sum(b.fparty_num) fspucnt
          from stage.channel_market_reg_mid a
          join stage.user_gameparty_info_mid b
            on a.fbpid = b.fbpid
           and a.fuid = b.fuid
           and a.dt = '%(ld_begin)s'
           and b.dt = '%(ld_begin)s'
          join dim.bpid_map c
            on a.fbpid = c.fbpid
          join analysis.dc_channel_package d
            on a.fnow_channel_id = d.fpkg_id
         group by fgamefsk,
                  fplatformfsk,
                  fversion_old,
                  fterminalfsk,
                  d.fpkg_channel_id
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        -- 今天注册玩牌的用户数
        -- 依赖表user_gameparty_info_mid 先完成
        drop table if exists analysis.user_channel_fct_tmp_pucnt_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_tmp_pucnt_%(num_begin)s as
        select '%(ld_begin)s' fdate, fgamefsk,fplatformfsk,fversion_old fversionfsk,fterminalfsk,
                fpkg_channel_id fchannel_id,
                count(distinct a.fuid ) fpun,                                            -- 玩牌用户数
                sum(a.fparty_num) fpucnt                                                     -- 玩牌局数
        from (select a.fbpid, fchannel_id, a.fuid, sum(b.fparty_num) fparty_num
                from stage.channel_market_active_mid a
                join stage.user_gameparty_info_mid b
                  on a.fbpid=b.fbpid
                 and a.fuid=b.fuid
                 and a.dt = '%(ld_begin)s'
                 and b.dt = '%(ld_begin)s'
                 group by a.fbpid, fchannel_id, a.fuid
             ) a
        join analysis.dc_channel_package b
          on a.fchannel_id = b.fpkg_id
        join dim.bpid_map d
          on a.fbpid=d.fbpid
        group By fgamefsk,fplatformfsk,fversion_old,fterminalfsk, fpkg_channel_id

        """ % self.hql_dict
        hql_list.append( hql )


        hql = """
        -- table analysis.user_channel_fct_gmprty_cnt_part
        drop table if exists analysis.user_channel_fct_gmprty_cnt_part_%(num_begin)s;
        create table if not exists analysis.user_channel_fct_gmprty_cnt_part_%(num_begin)s as
        select  fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fchannel_id,
        max(freg_gameparty_num) freg_gameparty_num,
        max(fspucnt) fspucnt,
        max(fpun) fpun,
        max(fpucnt) fpucnt
        from
        (
            select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fchannel_id,
            freg_gameparty_num,
            fspucnt,
            null fpun,
            null fpucnt
            from analysis.user_channel_fct_tmp_reg_gameparty_%(num_begin)s a
            union all
            select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fchannel_id,
            null freg_gameparty_num,
            null fspucnt,
            fpun,
            fpucnt
            from analysis.user_channel_fct_tmp_pucnt_%(num_begin)s b
        ) t
        group by fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fchannel_id
        """ % self.hql_dict
        hql_list.append( hql )

        hql = """
        drop table if exists analysis.user_channel_fct_tmp_reg_gameparty_%(num_begin)s;

        drop table if exists analysis.user_channel_fct_tmp_pucnt_%(num_begin)s;
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
    a = promotion_channel_gmprty_cnt(stat_date)
    a()
