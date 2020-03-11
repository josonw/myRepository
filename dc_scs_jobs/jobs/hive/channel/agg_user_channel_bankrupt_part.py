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


class agg_user_channel_bankrupt_part(BaseStatModel):
    def create_tab(self):

        hql = """
        create table if not exists dcnew.user_channel_bankrupt_part (
               fdate                  date,
               fgamefsk               bigint,
               fplatformfsk           bigint,
               fhallfsk               bigint,
               fsubgamefsk            bigint,
               fterminaltypefsk       bigint,
               fversionfsk            bigint,
               fchannelcode           bigint,
               fchannel_id            bigint   comment '渠道id',
               freg_ruptu             bigint   comment '新增破产用户数',
               freg_rupt_cnt          bigint   comment '新增破产次数',
               freg_rupt_relieveu     bigint   comment '新增破产接收救济用户数',
               freg_rupt_relieve_cnt  bigint   comment '新增破产接收救济次数',
               fact_ruptu             bigint   comment '活跃破产用户数',
               fact_rupt_cnt          bigint   comment '活跃破产次数',
               fact_rupt_relieveu     bigint   comment '活跃破产接收救济用户数',
               fact_rupt_relieve_cnt  bigint   comment '活跃破产接收救济次数',
               frupt_payu             bigint   comment '破产付费用户数',
               freg_gameparty_unum    bigint   comment '新增玩牌用户数',
               freg_gameparty_num     bigint   comment '新增玩牌次数',
               fact_gameparty_unum    bigint   comment '活跃玩牌用户数',
               fact_gameparty_num     bigint   comment '活跃玩牌次数'
               )comment '渠道破产相关'
               partitioned by(dt date)
        location '/dw/dcnew/user_channel_bankrupt_part'
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

        hql = """--新增用户破产救济
                  drop table if exists work.user_channel_bankrupt_part_tmp_a_%(statdatenum)s;
                create table work.user_channel_bankrupt_part_tmp_a_%(statdatenum)s as
                        select c.fgamefsk,
                               c.fplatformfsk,
                               c.fhallfsk,
                               c.fterminaltypefsk,
                               c.fversionfsk,
                               %(null_int_report)d fgame_id,
                               %(null_int_report)d fchannel_code,
                               c.hallmode,
                               a.fuid,
                               d.fpkg_channel_id fchannel_id,
                               b.frupt_cnt,
                               b.frlv_cnt,
                               e.fparty_num
                          from stage.channel_market_reg_mid a
                          left join (select fbpid,fuid,sum(frupt_cnt) frupt_cnt,sum(frlv_cnt) frlv_cnt
                                       from dim.user_bankrupt_relieve
                                      where dt = '%(statdate)s'
                                     group by fbpid,fuid
                                    ) b
                            on a.fbpid=b.fbpid
                           and a.fuid=b.fuid
                          left join (select fbpid,fuid,sum(fparty_num) fparty_num
                                       from dim.user_gameparty
                                      where dt = '%(statdate)s'
                                     group by fbpid,fuid
                                    ) e
                            on a.fbpid=e.fbpid
                           and a.fuid=e.fuid
                          join analysis.dc_channel_package d
                            on a.fnow_channel_id = d.fpkg_id
                          join dim.bpid_map c
                            on a.fbpid=c.fbpid
                         where a.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """ --活跃用户破产救济
                  drop table if exists work.user_channel_bankrupt_part_tmp_b_%(statdatenum)s;
                create table work.user_channel_bankrupt_part_tmp_b_%(statdatenum)s as
                        select c.fgamefsk,
                               c.fplatformfsk,
                               c.fhallfsk,
                               c.fterminaltypefsk,
                               c.fversionfsk,
                               %(null_int_report)d fgame_id,
                               %(null_int_report)d fchannel_code,
                               c.hallmode,
                               a.fuid,
                               d.fpkg_channel_id fchannel_id,
                               b.frupt_cnt,
                               b.frlv_cnt,
                               e.fparty_num
                          from stage.channel_market_active_mid a
                          left join (select fbpid,fuid,sum(frupt_cnt) frupt_cnt,sum(frlv_cnt) frlv_cnt
                                       from dim.user_bankrupt_relieve
                                      where dt = '%(statdate)s'
                                     group by fbpid,fuid
                                    ) b
                            on a.fbpid=b.fbpid
                           and a.fuid=b.fuid
                          left join (select fbpid,fuid,sum(fparty_num) fparty_num
                                       from dim.user_gameparty
                                      where dt = '%(statdate)s'
                                     group by fbpid,fuid
                                    ) e
                            on a.fbpid=e.fbpid
                           and a.fuid=e.fuid
                          join analysis.dc_channel_package d
                            on a.fchannel_id = d.fpkg_id
                          join dim.bpid_map c
                            on a.fbpid=c.fbpid
                                 where a.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--付费用户破产救济
                  drop table if exists work.user_channel_bankrupt_part_tmp_c_%(statdatenum)s;
                create table work.user_channel_bankrupt_part_tmp_c_%(statdatenum)s as
                        select c.fgamefsk,
                               c.fplatformfsk,
                               c.fhallfsk,
                               c.fterminaltypefsk,
                               c.fversionfsk,
                               %(null_int_report)d fgame_id,
                               %(null_int_report)d fchannel_code,
                               c.hallmode,
                               a.fuid,
                               d.fpkg_channel_id fchannel_id,
                               b.frupt_cnt,
                               b.frlv_cnt
                          from stage.channel_market_payment_mid a
                          left join (select fbpid,fuid,sum(frupt_cnt) frupt_cnt,sum(frlv_cnt) frlv_cnt
                                       from dim.user_bankrupt_relieve
                                      where dt = '%(statdate)s'
                                     group by fbpid,fuid
                                    ) b
                            on a.fbpid=b.fbpid
                           and a.fuid=b.fuid
                          join analysis.dc_channel_package d
                            on a.fchannel_id = d.fpkg_id
                          join dim.bpid_map c
                            on a.fbpid=c.fbpid
                                 where a.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select  fgamefsk
                       ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                       ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                       ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                       ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                       ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                       ,coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code
                       ,fchannel_id
                       ,count(distinct case when frupt_cnt >0 then fuid end) freg_ruptu                --新增破产用户数
                       ,sum(case when frupt_cnt >0 then frupt_cnt end) freg_rupt_cnt          --新增破产次数
                       ,count(distinct case when frlv_cnt >0 then fuid end) freg_rupt_relieveu         --新增破产接收救济用户数
                       ,sum(case when frlv_cnt >0 then frlv_cnt end) freg_rupt_relieve_cnt    --新增破产接收救济次数
                       ,0 fact_ruptu                   --活跃破产用户数
                       ,0 fact_rupt_cnt                --活跃破产次数
                       ,0 fact_rupt_relieveu           --活跃破产接收救济用户数
                       ,0 fact_rupt_relieve_cnt        --活跃破产接收救济次数
                       ,0 frupt_payu                   --破产付费用户数
                       ,count(distinct case when fparty_num >0 then fuid end) freg_gameparty_unum          --新增玩牌用户数
                       ,sum(case when fparty_num >0 then fparty_num end) freg_gameparty_num           --新增玩牌次数
                       ,0 fact_gameparty_unum          --活跃玩牌用户数
                       ,0 fact_gameparty_num           --活跃玩牌次数
                  from work.user_channel_bankrupt_part_tmp_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fchannel_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
                  drop table if exists work.user_channel_bankrupt_part_tmp_%(statdatenum)s;
                create table work.user_channel_bankrupt_part_tmp_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select  fgamefsk
                       ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                       ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                       ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                       ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                       ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                       ,coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code
                       ,fchannel_id
                       ,0 freg_ruptu                 --新增破产用户数
                       ,0 freg_rupt_cnt              --新增破产次数
                       ,0 freg_rupt_relieveu         --新增破产接收救济用户数
                       ,0 freg_rupt_relieve_cnt      --新增破产接收救济次数
                       ,count(distinct case when frupt_cnt >0 then fuid end)  fact_ruptu                   --活跃破产用户数
                       ,sum(case when frupt_cnt >0 then frupt_cnt end)  fact_rupt_cnt                --活跃破产次数
                       ,count(distinct case when frlv_cnt >0 then fuid end)  fact_rupt_relieveu           --活跃破产接收救济用户数
                       ,sum(case when frlv_cnt >0 then frlv_cnt end)  fact_rupt_relieve_cnt        --活跃破产接收救济次数
                       ,0 frupt_payu                   --破产付费用户数
                       ,0 freg_gameparty_unum          --新增玩牌用户数
                       ,0 freg_gameparty_num           --新增玩牌次数
                       ,count(distinct case when fparty_num >0 then fuid end) fact_gameparty_unum          --活跃玩牌用户数
                       ,sum(case when fparty_num >0 then fparty_num end) fact_gameparty_num           --活跃玩牌次数
                  from work.user_channel_bankrupt_part_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fchannel_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """ insert into table work.user_channel_bankrupt_part_tmp_%(statdatenum)s
        %(sql_template)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select  fgamefsk
                       ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                       ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                       ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                       ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                       ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                       ,coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code
                       ,fchannel_id
                       ,0 freg_ruptu                 --新增破产用户数
                       ,0 freg_rupt_cnt              --新增破产次数
                       ,0 freg_rupt_relieveu         --新增破产接收救济用户数
                       ,0 freg_rupt_relieve_cnt      --新增破产接收救济次数
                       ,0 fact_ruptu                   --活跃破产用户数
                       ,0 fact_rupt_cnt                --活跃破产次数
                       ,0 fact_rupt_relieveu           --活跃破产接收救济用户数
                       ,0 fact_rupt_relieve_cnt        --活跃破产接收救济次数
                       ,count(distinct case when frupt_cnt >0 then fuid end) frupt_payu       --破产付费用户数
                       ,0 freg_gameparty_unum          --新增玩牌用户数
                       ,0 freg_gameparty_num           --新增玩牌次数
                       ,0 fact_gameparty_unum          --活跃玩牌用户数
                       ,0 fact_gameparty_num           --活跃玩牌次数
                  from work.user_channel_bankrupt_part_tmp_c_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fchannel_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """ insert into table work.user_channel_bankrupt_part_tmp_%(statdatenum)s
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """insert overwrite table dcnew.user_channel_bankrupt_part partition( dt="%(statdate)s" )
                select "%(statdate)s" fdate,
                       fgamefsk,
                       fplatformfsk,
                       fhallfsk,
                       fgame_id,
                       fterminaltypefsk,
                       fversionfsk,
                       fchannel_code,
                       fchannel_id,
                       sum(freg_ruptu) freg_ruptu,
                       sum(freg_rupt_cnt) freg_rupt_cnt,
                       sum(freg_rupt_relieveu) freg_rupt_relieveu,
                       sum(freg_rupt_relieve_cnt) freg_rupt_relieve_cnt,
                       sum(fact_ruptu) fact_ruptu,
                       sum(fact_rupt_cnt) fact_rupt_cnt,
                       sum(fact_rupt_relieveu) fact_rupt_relieveu,
                       sum(fact_rupt_relieve_cnt) fact_rupt_relieve_cnt,
                       sum(frupt_payu) frupt_payu,
                       sum(freg_gameparty_unum) freg_gameparty_unum,
                       sum(freg_gameparty_num) freg_gameparty_num,
                       sum(fact_gameparty_unum) fact_gameparty_unum,
                       sum(fact_gameparty_num) fact_gameparty_num
                  from work.user_channel_bankrupt_part_tmp_%(statdatenum)s gs
                 group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,fchannel_id
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.user_channel_bankrupt_part_tmp_a_%(statdatenum)s;
                 drop table if exists work.user_channel_bankrupt_part_tmp_b_%(statdatenum)s;
                 drop table if exists work.user_channel_bankrupt_part_tmp_c_%(statdatenum)s;
                 drop table if exists work.user_channel_bankrupt_part_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_user_channel_bankrupt_part(sys.argv[1:])
a()
