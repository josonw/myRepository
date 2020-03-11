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


class agg_user_channel_user_info(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.user_channel_user_info (
               fdate                  date,
               fgamefsk               bigint,
               fplatformfsk           bigint,
               fhallfsk               bigint,
               fsubgamefsk            bigint,
               fterminaltypefsk       bigint,
               fversionfsk            bigint,
               fchannelcode           bigint,
               fchannel_id            bigint          comment '渠道id',
               fdsu                   bigint          comment '日新增',
               fdau                   bigint          comment '日活跃',
               fdpu                   bigint          comment '日付费',
               fdip                   decimal(20,2)   comment '日付费额度',
               ffirst_dpu             bigint          comment '日首次付费',
               ffirst_dip             decimal(20,2)   comment '日首次付费额度',
               fregdpu                bigint          comment '新增用户日付费',
               fregdip                decimal(20,2)   comment '新增用户日付费额度'
               )comment '渠道破产相关'
               partitioned by(dt date)
        location '/dw/dcnew/user_channel_user_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fchannel_id'],
                        'groups': [[1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0:
            return res

        hql = """--新增用户
                  drop table if exists work.user_channel_user_info_tmp_a_%(statdatenum)s;
                create table work.user_channel_user_info_tmp_a_%(statdatenum)s as
                        select c.fgamefsk,
                               c.fplatformfsk,
                               c.fhallfsk,
                               c.fterminaltypefsk,
                               c.fversionfsk,
                               %(null_int_report)d fgame_id,
                               %(null_int_report)d fchannel_code,
                               c.hallmode,
                               b.fpkg_channel_id fchannel_id,
                               a.fuid
                          from stage.channel_market_reg_mid a
                          join analysis.dc_channel_package b
                            on a.fnow_channel_id = b.fpkg_id
                          join dim.bpid_map c
                            on a.fbpid=c.fbpid
                         where a.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """ --活跃用户
                  drop table if exists work.user_channel_user_info_tmp_b_%(statdatenum)s;
                create table work.user_channel_user_info_tmp_b_%(statdatenum)s as
                        select c.fgamefsk,
                               c.fplatformfsk,
                               c.fhallfsk,
                               c.fterminaltypefsk,
                               c.fversionfsk,
                               %(null_int_report)d fgame_id,
                               %(null_int_report)d fchannel_code,
                               c.hallmode,
                               b.fpkg_channel_id fchannel_id,
                               a.fuid
                          from stage.channel_market_active_mid a
                          join analysis.dc_channel_package b
                            on a.fchannel_id = b.fpkg_id
                          join dim.bpid_map c
                            on a.fbpid=c.fbpid
                         where a.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--付费用户
                  drop table if exists work.user_channel_user_info_tmp_c_%(statdatenum)s;
                create table work.user_channel_user_info_tmp_c_%(statdatenum)s as
                        select c.fgamefsk,
                               c.fplatformfsk,
                               c.fhallfsk,
                               c.fterminaltypefsk,
                               c.fversionfsk,
                               %(null_int_report)d fgame_id,
                               %(null_int_report)d fchannel_code,
                               c.hallmode,
                               b.fpkg_channel_id fchannel_id,
                               a.fuid,
                               cast(round(a.fpay_money * a.fpay_rate,2) as decimal(20,2) ) fdip,
                               case when d.fuid is not null then 1 else 0 end is_first,
                               case when e.fuid is not null then 1 else 0 end is_reg
                          from stage.channel_market_payment_mid a
                          left join dim.user_pay d
                            on a.fbpid=d.fbpid
                           and a.fuid=d.fuid
                           and d.dt = '%(statdate)s'
                          left join stage.channel_market_reg_mid e
                            on a.fbpid=e.fbpid
                           and a.fuid=e.fuid
                           and e.dt = '%(statdate)s'
                          join analysis.dc_channel_package b
                            on a.fchannel_id = b.fpkg_id
                          join dim.bpid_map c
                            on a.fbpid=c.fbpid
                         where a.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--付费
                select  fgamefsk
                       ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                       ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                       ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                       ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                       ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                       ,coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code
                       ,fchannel_id
                       ,0 fdsu                   --日新增
                       ,0 fdau                   --日活跃
                       ,count(distinct fuid) fdpu                   --日付费
                       ,sum(fdip) fdip                   --日付费额度
                       ,count(distinct case when is_first = 1 then fuid end) ffirst_dpu             --日首次付费
                       ,sum(case when is_first = 1 then fdip end) ffirst_dip             --日首次付费额度
                       ,count(distinct case when is_reg = 1 then fuid end) fregdpu                --新增用户日付费
                       ,sum(case when is_reg = 1 then fdip end) fregdip                --新增用户日付费额度
                  from work.user_channel_user_info_tmp_c_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fchannel_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
                  drop table if exists work.user_channel_user_info_tmp_%(statdatenum)s;
                create table work.user_channel_user_info_tmp_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """ --新增
                select  fgamefsk
                       ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                       ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                       ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                       ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                       ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                       ,coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code
                       ,fchannel_id
                       ,count(distinct fuid) fdsu                   --日新增
                       ,0 fdau                   --日活跃
                       ,0 fdpu                   --日付费
                       ,0 fdip                   --日付费额度
                       ,0 ffirst_dpu             --日首次付费
                       ,0 ffirst_dip             --日首次付费额度
                       ,0 fregdpu                --新增用户日付费
                       ,0 fregdip                --新增用户日付费额度
                  from work.user_channel_user_info_tmp_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fchannel_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """ insert into table work.user_channel_user_info_tmp_%(statdatenum)s
        %(sql_template)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--活跃
                select  fgamefsk
                       ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                       ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                       ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                       ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                       ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                       ,coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code
                       ,fchannel_id
                       ,0 fdsu                   --日新增
                       ,count(distinct fuid) fdau                   --日活跃
                       ,0 fdpu                   --日付费
                       ,0 fdip                   --日付费额度
                       ,0 ffirst_dpu             --日首次付费
                       ,0 ffirst_dip             --日首次付费额度
                       ,0 fregdpu                --新增用户日付费
                       ,0 fregdip                --新增用户日付费额度
                  from work.user_channel_user_info_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fchannel_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """ insert into table work.user_channel_user_info_tmp_%(statdatenum)s
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """insert overwrite table dcnew.user_channel_user_info partition( dt="%(statdate)s" )
                select "%(statdate)s" fdate,
                       fgamefsk,
                       fplatformfsk,
                       fhallfsk,
                       fgame_id,
                       fterminaltypefsk,
                       fversionfsk,
                       fchannel_code,
                       fchannel_id,
                       sum(fdsu) fdsu,
                       sum(fdau) fdau,
                       sum(fdpu) fdpu,
                       sum(fdip) fdip,
                       sum(ffirst_dpu) ffirst_dpu,
                       sum(ffirst_dip) ffirst_dip,
                       sum(fregdpu) fregdpu,
                       sum(fregdip) fregdip
                  from work.user_channel_user_info_tmp_%(statdatenum)s gs
                 group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,fchannel_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.user_channel_user_info_tmp_a_%(statdatenum)s;
                 drop table if exists work.user_channel_user_info_tmp_b_%(statdatenum)s;
                 drop table if exists work.user_channel_user_info_tmp_c_%(statdatenum)s;
                 drop table if exists work.user_channel_user_info_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_user_channel_user_info(sys.argv[1:])
a()
