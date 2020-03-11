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


class agg_bud_gamecoin_balance(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_gamecoin_balance (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fgcoin_num          bigint     comment '游戏币所有用户结余',
               fgcoin_act_num      bigint     comment '游戏币活跃用户结余',
               fgcoin_bank_num     bigint     comment '游戏币保险箱结余'
               )comment '金流结余表'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_gamecoin_balance';
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
        extend_group_1 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id),
                               (fgamefsk, fgame_id) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk),
                               (fgamefsk, fplatformfsk, fhallfsk),
                               (fgamefsk, fplatformfsk) ) """

        query = {'extend_group_1': extend_group_1,
                 'extend_group_2': extend_group_2,
                 'null_str_group_rule': sql_const.NULL_STR_GROUP_RULE,
                 'null_int_group_rule': sql_const.NULL_INT_GROUP_RULE,
                 'statdatenum': """%(statdatenum)s""",
                 'statdate': """%(statdate)s"""}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--取基础指标
            drop table if exists work.bud_gamecoin_balance_tmp_1_%(statdatenum)s;
          create table work.bud_gamecoin_balance_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(a.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,t1.user_gamecoins_num fgamecoins
                 ,case when t2.fuid is not null then 1 else 0 end is_act  --是否活跃
            from dim.user_gamecoin_balance t1
            left join dim.user_gamecoin_balance_day a
              on t1.fbpid = a.fbpid
             and t1.fuid = a.fuid
             and a.dt = "%(statdate)s"
            left join (select distinct fbpid, fuid
                         from dim.user_act
                        where dt = "%(statdate)s"
                      ) t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and t1.user_gamecoins_num >0;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,sum(fgamecoins) fgcoin_num                                            --所有用户结余
                 ,sum(case when is_act = 1 then fgamecoins end) fgcoin_act_num      --活跃用户结余
                 ,cast (0 as bigint) fgcoin_bank_num                   --保险箱结余
            from work.bud_gamecoin_balance_tmp_1_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """drop table if exists work.bud_gamecoin_balance_tmp_%(statdatenum)s;
          create table work.bud_gamecoin_balance_tmp_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取保险箱数据
            drop table if exists work.bud_gamecoin_balance_tmp_2_%(statdatenum)s;
          create table work.bud_gamecoin_balance_tmp_2_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(a.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,t1.fbank_gamecoins_num
            from dim.user_bank_balance t1
            left join dim.user_bank_balance_day a
              on t1.fbpid = a.fbpid
             and t1.fuid = a.fuid
             and a.fcurrencies_type = '0'
             and a.dt = "%(statdate)s"
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and t1.fcurrencies_type = '0'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总2
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,0 fgcoin_num          --所有用户结余
                 ,0 fgcoin_act_num      --活跃用户结余
                 ,sum(fbank_gamecoins_num) fgcoin_bank_num                   --保险箱结余
            from work.bud_gamecoin_balance_tmp_2_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert into table work.bud_gamecoin_balance_tmp_%(statdatenum)s """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 插入目标表
        hql = """  insert overwrite table bud_dm.bud_gamecoin_balance  partition(dt='%(statdate)s')
                   select "%(statdate)s" fdate
                          ,fgamefsk
                          ,fplatformfsk
                          ,fhallfsk
                          ,fgame_id
                          ,fterminaltypefsk
                          ,fversionfsk
                          ,max(fgcoin_num) fgcoin_num            --所有用户结余
                          ,max(fgcoin_act_num) fgcoin_act_num    --活跃用户结余
                          ,max(fgcoin_bank_num) fgcoin_bank_num  --保险箱结余
                     from work.bud_gamecoin_balance_tmp_%(statdatenum)s
                    group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_gamecoin_balance_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_gamecoin_balance_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_gamecoin_balance_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_gamecoin_balance(sys.argv[1:])
a()
