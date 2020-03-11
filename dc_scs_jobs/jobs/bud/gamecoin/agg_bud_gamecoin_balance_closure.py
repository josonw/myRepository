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


class agg_bud_gamecoin_balance_closure(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_gamecoin_balance_closure (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fcointype           varchar(50)      comment '货币类型',
               fbalance            bigint     comment '所有用户结余',
               fbalance_bank       bigint     comment '保险箱结余'
               )comment '封停用户金流结余表'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_gamecoin_balance_closure';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        # 两组组合，共4种

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--取基础指标
            drop table if exists work.bud_gamecoin_balance_closure_tmp_1_%(statdatenum)s;
          create table work.bud_gamecoin_balance_closure_tmp_1_%(statdatenum)s as
          select t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,%(null_int_group_rule)d fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,t1.fuid
                 ,t1.fgamecoins_num
                 ,t1.fbank_gamecoins_num
            from dim.user_gamecoin_balance_array t1
            join (select t.*,tt.fhallfsk
                    from veda.dfqp_user_portrait_basic t
                    join dim.bpid_map tt
                      on t.signup_bpid = tt.fbpid
                   where t.user_status in ('永久封停','临时封停')
                 )  t3
              on t1.fuid = t3.mid
             and t1.fhallfsk = t3.fhallfsk
           where t1.dt = '%(statdate)s'
             and t1.fgamecoins_num + fbank_gamecoins_num >0;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """insert overwrite table bud_dm.bud_gamecoin_balance_closure  partition(dt='%(statdate)s')
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       '1' fcointype,
                       sum(fgamecoins_num) fbalance,
                       sum(fbank_gamecoins_num) fbalance_bank
                  from work.bud_gamecoin_balance_closure_tmp_1_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取基础指标
            drop table if exists work.bud_gamecoin_balance_closure_tmp_2_%(statdatenum)s;
          create table work.bud_gamecoin_balance_closure_tmp_2_%(statdatenum)s as
          select t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,%(null_int_group_rule)d fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,t1.fuid
                 ,t1.fcurrencies_type
                 ,t1.fcurrencies_num
                 ,t1.fbank_gamecoins_num
            from dim.user_currencies_balance_array t1
            join (select t.*,tt.fhallfsk
                    from veda.dfqp_user_portrait_basic t
                    join dim.bpid_map tt
                      on t.signup_bpid = tt.fbpid
                   where t.user_status in ('永久封停','临时封停')
                 )  t3
              on t1.fuid = t3.mid
             and t1.fhallfsk = t3.fhallfsk
           where t1.dt = '%(statdate)s'
             and t1.fcurrencies_num + fbank_gamecoins_num >0
             and t1.fcurrencies_type = '11';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """insert into table bud_dm.bud_gamecoin_balance_closure partition(dt='%(statdate)s')
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       fcurrencies_type fcointype,
                       sum(fcurrencies_num) fbalance,
                       sum(fbank_gamecoins_num) fbalance_bank
                  from work.bud_gamecoin_balance_closure_tmp_2_%(statdatenum)s gs
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fcurrencies_type
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_gamecoin_balance_closure_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_gamecoin_balance_closure_tmp_2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_gamecoin_balance_closure(sys.argv[1:])
a()
