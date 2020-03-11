# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_channel_verinfo_info(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.channel_verinfo_info
        (
          fdate               date,
          fgamefsk            bigint,
          fplatformfsk        bigint,
          fhallfsk            bigint,
          fsubgamefsk         bigint,
          fterminaltypefsk    bigint,
          fversionfsk         bigint,
          fchannelcode        bigint,
          fpkg_channel_id     varchar(64),
          fchannel_id         varchar(64),
          fcli_verinfo        varchar(100),
          freg_num            bigint,
          factive_num         bigint
        )
        partitioned by (dt date);
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fpkg_channel_id', 'fchannel_id', 'fcli_verinfo'],
                        'groups': [[1, 1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0:
            return res

        hql = """
        use analysis;
        create view if not exists dc_channel_package as
        select fid fpkg_id, ftrader_id fpkg_channel_id, fname fpkg_desc
        from analysis.marketing_channel_pkg_info;

        use analysis;
        create view if not exists dc_channel as
        select fid fchannel_id, fname name from analysis.marketing_channel_trader_info;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--新增用户
                  drop table if exists work.channel_verinfo_info_tmp_a_%(statdatenum)s;
                create table work.channel_verinfo_info_tmp_a_%(statdatenum)s as
                        select c.fgamefsk,
                               c.fplatformfsk,
                               c.fhallfsk,
                               c.fterminaltypefsk,
                               c.fversionfsk,
                               %(null_int_report)d fgame_id,
                               %(null_int_report)d fchannel_code,
                               c.hallmode,
                               fpkg_channel_id,
                               fchannel_id,
                               fcli_verinfo,
                               a.fuid
                          from stage.channel_market_reg_mid a
                          join analysis.dc_channel_package b
                            on a.fchannel_id = b.fpkg_id
                          join dim.bpid_map c
                            on a.fbpid = c.fbpid
                         where a.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--活跃用户
                  drop table if exists work.channel_verinfo_info_tmp_b_%(statdatenum)s;
                create table work.channel_verinfo_info_tmp_b_%(statdatenum)s as
                        select c.fgamefsk,
                               c.fplatformfsk,
                               c.fhallfsk,
                               c.fterminaltypefsk,
                               c.fversionfsk,
                               %(null_int_report)d fgame_id,
                               %(null_int_report)d fchannel_code,
                               c.hallmode,
                               fpkg_channel_id,
                               fchannel_id,
                               fcli_verinfo,
                               a.fuid
                          from stage.channel_market_active_mid a
                          join analysis.dc_channel_package b
                            on a.fchannel_id = b.fpkg_id
                          join dim.bpid_map c
                            on a.fbpid = c.fbpid
                         where a.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fpkg_channel_id,
                       fchannel_id,
                       fcli_verinfo,
                       count(distinct fuid) freg_num,
                       0 factive_num
                  from work.channel_verinfo_info_tmp_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,
                       fpkg_channel_id,
                       fchannel_id,
                       fcli_verinfo
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
                  drop table if exists work.channel_verinfo_info_tmp_%(statdatenum)s;
                create table work.channel_verinfo_info_tmp_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fpkg_channel_id,
                       fchannel_id,
                       fcli_verinfo,
                       0 freg_num,
                       count(distinct fuid) factive_num
                  from work.channel_verinfo_info_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,
                       fpkg_channel_id,
                       fchannel_id,
                       fcli_verinfo
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """insert into table work.channel_verinfo_info_tmp_%(statdatenum)s
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """insert overwrite table dcnew.channel_verinfo_info partition( dt="%(statdate)s" )
                select "%(statdate)s" fdate,
                       fgamefsk,
                       fplatformfsk,
                       fhallfsk,
                       fgame_id,
                       fterminaltypefsk,
                       fversionfsk,
                       fchannel_code,
                       fpkg_channel_id,
                       fchannel_id,
                       fcli_verinfo,
                       sum(freg_num) freg_num,
                       sum(factive_num) factive_num
                  from work.channel_verinfo_info_tmp_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,
                       fpkg_channel_id,
                       fchannel_id,
                       fcli_verinfo
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.channel_verinfo_info_tmp_%(statdatenum)s;
                 drop table if exists work.channel_verinfo_info_tmp_a_%(statdatenum)s;
                 drop table if exists work.channel_verinfo_info_tmp_b_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


# 生成统计实例
a = agg_channel_verinfo_info(sys.argv[1:])
a()
