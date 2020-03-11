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


class agg_bud_all_currencies_before(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_all_currencies_dis (
               fdate                   date,
               fgamefsk                bigint,
               fplatformfsk            bigint,
               fhallfsk                bigint,
               fsubgamefsk             bigint,
               fterminaltypefsk        bigint,
               fversionfsk             bigint,
               fchannelcode            bigint,
               fcurrencies_type        varchar(10)   comment '用户货币类型',
               fact_unum               bigint        comment '活跃用户数',
               fact_num                bigint        comment '活跃货币数',
               freg_unum               bigint        comment '新增用户数',
               freg_num                bigint        comment '新增货币数',
               fpay_unum               bigint        comment '付费用户数',
               fpay_num                bigint        comment '付费货币数',
               fbankrupt_unum          bigint        comment '破产用户数',
               fbankrupt_num           bigint        comment '破产货币数',
               fall_unum               bigint        comment '所有用户数',
               fall_num                bigint        comment '所有货币数',
               fpaid_unum              bigint        comment '历史付费活跃用户数',
               fpaid_num               bigint        comment '历史付费活跃货币数',
               flft                    bigint        comment '区间下界',
               frgt                    bigint        comment '区间上界'
               )comment '保险箱加携带游戏币分布'
               partitioned by(dt string)
        location '/dw/bud_dm/bud_all_currencies_dis';

        create table if not exists bud_dm.bud_bring_currencies_dis (
               fdate                   date,
               fgamefsk                bigint,
               fplatformfsk            bigint,
               fhallfsk                bigint,
               fsubgamefsk             bigint,
               fterminaltypefsk        bigint,
               fversionfsk             bigint,
               fchannelcode            bigint,
               fcurrencies_type        varchar(10)   comment '用户货币类型',
               fact_unum               bigint        comment '活跃用户数',
               fact_num                bigint        comment '活跃货币数',
               freg_unum               bigint        comment '新增用户数',
               freg_num                bigint        comment '新增货币数',
               fpay_unum               bigint        comment '付费用户数',
               fpay_num                bigint        comment '付费货币数',
               fbankrupt_unum          bigint        comment '破产用户数',
               fbankrupt_num           bigint        comment '破产货币数',
               fall_unum               bigint        comment '所有用户数',
               fall_num                bigint        comment '所有货币数',
               fpaid_unum              bigint        comment '历史付费活跃用户数',
               fpaid_num               bigint        comment '历史付费活跃货币数',
               flft                    bigint        comment '区间下界',
               frgt                    bigint        comment '区间上界'
               )comment '携带游戏币分布'
               partitioned by(dt string)
        location '/dw/bud_dm/bud_bring_currencies_dis';

        create table if not exists bud_dm.bud_bank_currencies_dis (
               fdate                   date,
               fgamefsk                bigint,
               fplatformfsk            bigint,
               fhallfsk                bigint,
               fsubgamefsk             bigint,
               fterminaltypefsk        bigint,
               fversionfsk             bigint,
               fchannelcode            bigint,
               fcurrencies_type        varchar(10)   comment '用户货币类型',
               fact_unum               bigint        comment '活跃用户数',
               fact_num                bigint        comment '活跃货币数',
               freg_unum               bigint        comment '新增用户数',
               freg_num                bigint        comment '新增货币数',
               fpay_unum               bigint        comment '付费用户数',
               fpay_num                bigint        comment '付费货币数',
               fbankrupt_unum          bigint        comment '破产用户数',
               fbankrupt_num           bigint        comment '破产货币数',
               fall_unum               bigint        comment '所有用户数',
               fall_num                bigint        comment '所有货币数',
               fpaid_unum              bigint        comment '历史付费活跃用户数',
               fpaid_num               bigint        comment '历史付费活跃货币数',
               flft                    bigint        comment '区间下界',
               frgt                    bigint        comment '区间上界'
               )comment '保险箱游戏币分布'
               partitioned by(dt string)
        location '/dw/bud_dm/bud_bank_currencies_dis';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """ --活跃用户
            drop table if exists work.bud_all_currencies_dis_tmp_2_%(statdatenum)s;
          create table work.bud_all_currencies_dis_tmp_2_%(statdatenum)s as
            select fgamefsk
                   ,fplatformfsk
                   ,fhallfsk
                   ,fsubgamefsk fgame_id
                   ,fterminaltypefsk
                   ,fversionfsk
                   ,'act_user' fuser_type
                   ,fuid
                   ,0 num
              from dim.user_act_array t
             where t.dt = "%(statdate)s";
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """ --新增用户
            insert into work.bud_all_currencies_dis_tmp_2_%(statdatenum)s
            select fgamefsk
                   ,fplatformfsk
                   ,fhallfsk
                   ,fgame_id
                   ,fterminaltypefsk
                   ,fversionfsk
                   ,'reg_user' fuser_type
                   ,fuid
                   ,0 num
              from dim.reg_user_array t
             where t.dt = "%(statdate)s";
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """ --付费用户
            insert into work.bud_all_currencies_dis_tmp_2_%(statdatenum)s
            select fgamefsk
                   ,fplatformfsk
                   ,fhallfsk
                   ,fsubgamefsk fgame_id
                   ,fterminaltypefsk
                   ,fversionfsk
                   ,'pay_user' fuser_type
                   ,fuid
                   ,0 num
              from dim.user_pay_array t
             where t.dt = "%(statdate)s";
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """ --破产用户
            insert into work.bud_all_currencies_dis_tmp_2_%(statdatenum)s
            select fgamefsk
                   ,fplatformfsk
                   ,fhallfsk
                   ,fsubgamefsk fgame_id
                   ,fterminaltypefsk
                   ,fversionfsk
                   ,'rupt_user' fuser_type
                   ,fuid
                   ,0 num
              from dim.user_bankrupt_array t
             where t.dt = "%(statdate)s";
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """ --历史付费活跃
            insert into work.bud_all_currencies_dis_tmp_2_%(statdatenum)s
            select fgamefsk
                   ,fplatformfsk
                   ,fhallfsk
                   ,fsubgamefsk fgame_id
                   ,fterminaltypefsk
                   ,fversionfsk
                   ,'paid_user' fuser_type
                   ,fuid
                   ,0 num
              from dim.user_act_paid_array t
             where t.dt = "%(statdate)s";
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--所有用户
            insert into work.bud_all_currencies_dis_tmp_2_%(statdatenum)s
            select fgamefsk
                   ,fplatformfsk
                   ,fhallfsk
                   ,fsubgamefsk fgame_id
                   ,fterminaltypefsk
                   ,fversionfsk
                   ,'all_user' fuser_type
                   ,fuid
                   ,0 num
              from dim.user_currencies_balance_array t
             where t.dt = "%(statdate)s";
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--所有用户
            drop table if exists work.bud_all_currencies_dis_tmp_%(statdatenum)s;
          create table work.bud_all_currencies_dis_tmp_%(statdatenum)s as
          select t.fgamefsk
                 ,t.fplatformfsk
                 ,t.fhallfsk
                 ,t.fgame_id
                 ,t.fterminaltypefsk
                 ,t.fversionfsk
                 ,t.fuser_type
                 ,t.fuid
                 ,t.num
                 ,t1.fcurrencies_type
                 ,coalesce(t1.fcurrencies_num, 0) fcurrencies_num
                 ,coalesce(t1.fbank_gamecoins_num, 0) fbank_gamecoins_num
                 ,case when coalesce(t1.fcurrencies_num, 0) =0 then 0
                      when fcurrencies_num < 1000 then (ceil(fcurrencies_num/50)-1) * 50
                      when fcurrencies_num < 10000 then (ceil(fcurrencies_num/500)-1) * 500
                      when fcurrencies_num < 100000 then (ceil(fcurrencies_num/5000)-1) * 5000
                      when fcurrencies_num < 1000000 then (ceil(fcurrencies_num/50000)-1) * 50000
                      when fcurrencies_num < 10000000 then (ceil(fcurrencies_num/500000)-1) * 500000
                      when fcurrencies_num < 100000000 then (ceil(fcurrencies_num/5000000)-1) * 5000000
                      when fcurrencies_num < 1000000000 then (ceil(fcurrencies_num/50000000)-1) * 50000000
                      when fcurrencies_num < 10000000000 then (ceil(fcurrencies_num/500000000)-1) * 500000000
                      when fcurrencies_num < 15000000000 then (ceil(fcurrencies_num/1000000000)-1) * 1000000000
                      else 15000000000 end br_flft
                 ,case when coalesce(t1.fcurrencies_num, 0) =0 then 0
                      when fcurrencies_num < 1000 then ceil(fcurrencies_num/50) * 50
                      when fcurrencies_num < 10000 then ceil(fcurrencies_num/500) * 500
                      when fcurrencies_num < 100000 then ceil(fcurrencies_num/5000) * 5000
                      when fcurrencies_num < 1000000 then ceil(fcurrencies_num/50000) * 50000
                      when fcurrencies_num < 10000000 then ceil(fcurrencies_num/500000) * 500000
                      when fcurrencies_num < 100000000 then ceil(fcurrencies_num/5000000) * 5000000
                      when fcurrencies_num < 1000000000 then ceil(fcurrencies_num/50000000) * 50000000
                      when fcurrencies_num < 10000000000 then ceil(fcurrencies_num/500000000) * 500000000
                      when fcurrencies_num < 15000000000 then ceil(fcurrencies_num/1000000000) * 1000000000
                      else 10000000000000000 end br_frgt
                 ,case when coalesce(t1.fbank_gamecoins_num, 0) =0 then 0
                      when fbank_gamecoins_num < 1000 then (ceil(fbank_gamecoins_num/50)-1) * 50
                      when fbank_gamecoins_num < 10000 then (ceil(fbank_gamecoins_num/500)-1) * 500
                      when fbank_gamecoins_num < 100000 then (ceil(fbank_gamecoins_num/5000)-1) * 5000
                      when fbank_gamecoins_num < 1000000 then (ceil(fbank_gamecoins_num/50000)-1) * 50000
                      when fbank_gamecoins_num < 10000000 then (ceil(fbank_gamecoins_num/500000)-1) * 500000
                      when fbank_gamecoins_num < 100000000 then (ceil(fbank_gamecoins_num/5000000)-1) * 5000000
                      when fbank_gamecoins_num < 1000000000 then (ceil(fbank_gamecoins_num/50000000)-1) * 50000000
                      when fbank_gamecoins_num < 10000000000 then (ceil(fbank_gamecoins_num/500000000)-1) * 500000000
                      when fbank_gamecoins_num < 15000000000 then (ceil(fbank_gamecoins_num/1000000000)-1) * 1000000000
                      else 15000000000 end ba_flft
                 ,case when coalesce(t1.fbank_gamecoins_num, 0) =0 then 0
                      when fbank_gamecoins_num < 1000 then ceil(fbank_gamecoins_num/50) * 50
                      when fbank_gamecoins_num < 10000 then ceil(fbank_gamecoins_num/500) * 500
                      when fbank_gamecoins_num < 100000 then ceil(fbank_gamecoins_num/5000) * 5000
                      when fbank_gamecoins_num < 1000000 then ceil(fbank_gamecoins_num/50000) * 50000
                      when fbank_gamecoins_num < 10000000 then ceil(fbank_gamecoins_num/500000) * 500000
                      when fbank_gamecoins_num < 100000000 then ceil(fbank_gamecoins_num/5000000) * 5000000
                      when fbank_gamecoins_num < 1000000000 then ceil(fbank_gamecoins_num/50000000) * 50000000
                      when fbank_gamecoins_num < 10000000000 then ceil(fbank_gamecoins_num/500000000) * 500000000
                      when fbank_gamecoins_num < 15000000000 then ceil(fbank_gamecoins_num/1000000000) * 1000000000
                      else 10000000000000000 end ba_frgt
                 ,case when coalesce(t1.fbank_gamecoins_num, 0)+ coalesce(t1.fcurrencies_num, 0) =0 then 0
                      when fbank_gamecoins_num + fcurrencies_num < 1000 then (ceil((fbank_gamecoins_num + fcurrencies_num)/50)-1) * 50
                      when fbank_gamecoins_num + fcurrencies_num < 10000 then (ceil((fbank_gamecoins_num + fcurrencies_num)/500)-1) * 500
                      when fbank_gamecoins_num + fcurrencies_num < 100000 then (ceil((fbank_gamecoins_num + fcurrencies_num)/5000)-1) * 5000
                      when fbank_gamecoins_num + fcurrencies_num < 1000000 then (ceil((fbank_gamecoins_num + fcurrencies_num)/50000)-1) * 50000
                      when fbank_gamecoins_num + fcurrencies_num < 10000000 then (ceil((fbank_gamecoins_num + fcurrencies_num)/500000)-1) * 500000
                      when fbank_gamecoins_num + fcurrencies_num < 100000000 then (ceil((fbank_gamecoins_num + fcurrencies_num)/5000000)-1) * 5000000
                      when fbank_gamecoins_num + fcurrencies_num < 1000000000 then (ceil((fbank_gamecoins_num + fcurrencies_num)/50000000)-1) * 50000000
                      when fbank_gamecoins_num + fcurrencies_num < 10000000000 then (ceil((fbank_gamecoins_num + fcurrencies_num)/500000000)-1) * 500000000
                      when fbank_gamecoins_num + fcurrencies_num < 15000000000 then (ceil((fbank_gamecoins_num + fcurrencies_num)/1000000000)-1) * 1000000000
                      else 15000000000 end all_flft
                 ,case when coalesce(t1.fbank_gamecoins_num, 0)+ coalesce(t1.fcurrencies_num, 0) =0 then 0
                      when fbank_gamecoins_num + fcurrencies_num < 1000 then ceil((fbank_gamecoins_num + fcurrencies_num)/50) * 50
                      when fbank_gamecoins_num + fcurrencies_num < 10000 then ceil((fbank_gamecoins_num + fcurrencies_num)/500) * 500
                      when fbank_gamecoins_num + fcurrencies_num < 100000 then ceil((fbank_gamecoins_num + fcurrencies_num)/5000) * 5000
                      when fbank_gamecoins_num + fcurrencies_num < 1000000 then ceil((fbank_gamecoins_num + fcurrencies_num)/50000) * 50000
                      when fbank_gamecoins_num + fcurrencies_num < 10000000 then ceil((fbank_gamecoins_num + fcurrencies_num)/500000) * 500000
                      when fbank_gamecoins_num + fcurrencies_num < 100000000 then ceil((fbank_gamecoins_num + fcurrencies_num)/5000000) * 5000000
                      when fbank_gamecoins_num + fcurrencies_num < 1000000000 then ceil((fbank_gamecoins_num + fcurrencies_num)/50000000) * 50000000
                      when fbank_gamecoins_num + fcurrencies_num < 10000000000 then ceil((fbank_gamecoins_num + fcurrencies_num)/500000000) * 500000000
                      when fbank_gamecoins_num + fcurrencies_num < 15000000000 then ceil((fbank_gamecoins_num + fcurrencies_num)/1000000000) * 1000000000
                      else 10000000000000000 end all_frgt
            from work.bud_all_currencies_dis_tmp_2_%(statdatenum)s t
            left join dim.user_currencies_balance_array t1
              on t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t.fuid = t1.fuid
             and t1.dt = "%(statdate)s" ;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_all_currencies_before(sys.argv[1:])
a()
