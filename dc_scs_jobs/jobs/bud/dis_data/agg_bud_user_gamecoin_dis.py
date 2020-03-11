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


class agg_bud_user_gamecoin_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_gamecoin_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuser_type          varchar(20)      comment '用户类型:reg_user,act_user',
               fdata_type          varchar(20)      comment '数据类型:unum-用户,num-总金额',
               num_0               bigint           comment '0',
               num_1000            bigint           comment '(1,1000]',
               num_5000            bigint           comment '(1000,5000]',
               num_10000           bigint           comment '(5000,10000]',
               num_50000           bigint           comment '(10000,50000]',
               num_100000          bigint           comment '(50000,100000]',
               num_500000          bigint           comment '(100000,500000]',
               num_1000000         bigint           comment '(500000,1000000]',
               num_5000000         bigint           comment '(1000000,5000000]',
               num_10000000        bigint           comment '(5000000,10000000]',
               num_50000000        bigint           comment '(10000000,50000000]',
               num_100000000       bigint           comment '(50000000,100000000]',
               num_1000000000      bigint           comment '(100000000,1000000000]',
               num_m1000000000     bigint           comment '>1000000000'
               )comment '用户游戏币分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_gamecoin_dis';
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

        # 汇总 --总金额
        hql = """
         insert overwrite table bud_dm.bud_user_gamecoin_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,'num_all' fdata_type
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num = 0 then fgamecoins_num + fbank_gamecoins_num end) num_0
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num between 1 and 1000 then fgamecoins_num + fbank_gamecoins_num end) num_1000
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num between 1001 and 5000 then fgamecoins_num + fbank_gamecoins_num end) num_5000
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num between 5001 and 10000 then fgamecoins_num + fbank_gamecoins_num end) num_10000
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num between 10001 and 50000 then fgamecoins_num + fbank_gamecoins_num end) num_50000
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num between 50001 and 100000 then fgamecoins_num + fbank_gamecoins_num end) num_100000
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num between 100001 and 500000 then fgamecoins_num + fbank_gamecoins_num end) num_500000
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num between 500001 and 1000000 then fgamecoins_num + fbank_gamecoins_num end) num_1000000
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num between 1000001 and 5000000 then fgamecoins_num + fbank_gamecoins_num end) num_5000000
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num between 5000001 and 10000000 then fgamecoins_num + fbank_gamecoins_num end) num_10000000
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num between 10000001 and 50000000 then fgamecoins_num + fbank_gamecoins_num end) num_50000000
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num between 50000001 and 100000000 then fgamecoins_num + fbank_gamecoins_num end) num_100000000
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num between 100000001 and 1000000000 then fgamecoins_num + fbank_gamecoins_num end) num_1000000000
                 ,sum(case when fgamecoins_num + fbank_gamecoins_num > 1000000000 then fgamecoins_num + fbank_gamecoins_num end) num_m1000000000
            from work.bud_all_gamecoin_dis_tmp_%(statdatenum)s t
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,fuser_type;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """
         insert into table bud_dm.bud_user_gamecoin_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,'unum_all' fdata_type
                 ,count(case when fgamecoins_num + fbank_gamecoins_num = 0 then t.fuid end) num_0
                 ,count(case when fgamecoins_num + fbank_gamecoins_num between 1 and 1000 then t.fuid end) num_1000
                 ,count(case when fgamecoins_num + fbank_gamecoins_num between 1001 and 5000 then t.fuid end) num_5000
                 ,count(case when fgamecoins_num + fbank_gamecoins_num between 5001 and 10000 then t.fuid end) num_10000
                 ,count(case when fgamecoins_num + fbank_gamecoins_num between 10001 and 50000 then t.fuid end) num_50000
                 ,count(case when fgamecoins_num + fbank_gamecoins_num between 50001 and 100000 then t.fuid end) num_100000
                 ,count(case when fgamecoins_num + fbank_gamecoins_num between 100001 and 500000 then t.fuid end) num_500000
                 ,count(case when fgamecoins_num + fbank_gamecoins_num between 500001 and 1000000 then t.fuid end) num_1000000
                 ,count(case when fgamecoins_num + fbank_gamecoins_num between 1000001 and 5000000 then t.fuid end) num_5000000
                 ,count(case when fgamecoins_num + fbank_gamecoins_num between 5000001 and 10000000 then t.fuid end) num_10000000
                 ,count(case when fgamecoins_num + fbank_gamecoins_num between 10000001 and 50000000 then t.fuid end) num_50000000
                 ,count(case when fgamecoins_num + fbank_gamecoins_num between 50000001 and 100000000 then t.fuid end) num_100000000
                 ,count(case when fgamecoins_num + fbank_gamecoins_num between 100000001 and 1000000000 then t.fuid end) num_1000000000
                 ,count(case when fgamecoins_num + fbank_gamecoins_num > 1000000000 then t.fuid end) num_m1000000000
            from work.bud_all_gamecoin_dis_tmp_%(statdatenum)s t
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,fuser_type;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """
         insert into table bud_dm.bud_user_gamecoin_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,'num_bring' fdata_type
                 ,sum(case when fgamecoins_num = 0 then fgamecoins_num end) num_0
                 ,sum(case when fgamecoins_num between 1 and 1000 then fgamecoins_num end) num_1000
                 ,sum(case when fgamecoins_num between 1001 and 5000 then fgamecoins_num end) num_5000
                 ,sum(case when fgamecoins_num between 5001 and 10000 then fgamecoins_num end) num_10000
                 ,sum(case when fgamecoins_num between 10001 and 50000 then fgamecoins_num end) num_50000
                 ,sum(case when fgamecoins_num between 50001 and 100000 then fgamecoins_num end) num_100000
                 ,sum(case when fgamecoins_num between 100001 and 500000 then fgamecoins_num end) num_500000
                 ,sum(case when fgamecoins_num between 500001 and 1000000 then fgamecoins_num end) num_1000000
                 ,sum(case when fgamecoins_num between 1000001 and 5000000 then fgamecoins_num end) num_5000000
                 ,sum(case when fgamecoins_num between 5000001 and 10000000 then fgamecoins_num end) num_10000000
                 ,sum(case when fgamecoins_num between 10000001 and 50000000 then fgamecoins_num end) num_50000000
                 ,sum(case when fgamecoins_num between 50000001 and 100000000 then fgamecoins_num end) num_100000000
                 ,sum(case when fgamecoins_num between 100000001 and 1000000000 then fgamecoins_num end) num_1000000000
                 ,sum(case when fgamecoins_num > 1000000000 then fgamecoins_num end) num_m1000000000
            from work.bud_all_gamecoin_dis_tmp_%(statdatenum)s t
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,fuser_type;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """
         insert into table bud_dm.bud_user_gamecoin_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,'unum_bring' fdata_type
                 ,count(case when fgamecoins_num = 0 then t.fuid end) num_0
                 ,count(case when fgamecoins_num between 1 and 1000 then t.fuid end) num_1000
                 ,count(case when fgamecoins_num between 1001 and 5000 then t.fuid end) num_5000
                 ,count(case when fgamecoins_num between 5001 and 10000 then t.fuid end) num_10000
                 ,count(case when fgamecoins_num between 10001 and 50000 then t.fuid end) num_50000
                 ,count(case when fgamecoins_num between 50001 and 100000 then t.fuid end) num_100000
                 ,count(case when fgamecoins_num between 100001 and 500000 then t.fuid end) num_500000
                 ,count(case when fgamecoins_num between 500001 and 1000000 then t.fuid end) num_1000000
                 ,count(case when fgamecoins_num between 1000001 and 5000000 then t.fuid end) num_5000000
                 ,count(case when fgamecoins_num between 5000001 and 10000000 then t.fuid end) num_10000000
                 ,count(case when fgamecoins_num between 10000001 and 50000000 then t.fuid end) num_50000000
                 ,count(case when fgamecoins_num between 50000001 and 100000000 then t.fuid end) num_100000000
                 ,count(case when fgamecoins_num between 100000001 and 1000000000 then t.fuid end) num_1000000000
                 ,count(case when fgamecoins_num > 1000000000 then t.fuid end) num_m1000000000
            from work.bud_all_gamecoin_dis_tmp_%(statdatenum)s t
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,fuser_type;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """
         insert into table bud_dm.bud_user_gamecoin_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,'num_bank' fdata_type
                 ,sum(case when fbank_gamecoins_num = 0 then fbank_gamecoins_num end) num_0
                 ,sum(case when fbank_gamecoins_num between 1 and 1000 then fbank_gamecoins_num end) num_1000
                 ,sum(case when fbank_gamecoins_num between 1001 and 5000 then fbank_gamecoins_num end) num_5000
                 ,sum(case when fbank_gamecoins_num between 5001 and 10000 then fbank_gamecoins_num end) num_10000
                 ,sum(case when fbank_gamecoins_num between 10001 and 50000 then fbank_gamecoins_num end) num_50000
                 ,sum(case when fbank_gamecoins_num between 50001 and 100000 then fbank_gamecoins_num end) num_100000
                 ,sum(case when fbank_gamecoins_num between 100001 and 500000 then fbank_gamecoins_num end) num_500000
                 ,sum(case when fbank_gamecoins_num between 500001 and 1000000 then fbank_gamecoins_num end) num_1000000
                 ,sum(case when fbank_gamecoins_num between 1000001 and 5000000 then fbank_gamecoins_num end) num_5000000
                 ,sum(case when fbank_gamecoins_num between 5000001 and 10000000 then fbank_gamecoins_num end) num_10000000
                 ,sum(case when fbank_gamecoins_num between 10000001 and 50000000 then fbank_gamecoins_num end) num_50000000
                 ,sum(case when fbank_gamecoins_num between 50000001 and 100000000 then fbank_gamecoins_num end) num_100000000
                 ,sum(case when fbank_gamecoins_num between 100000001 and 1000000000 then fbank_gamecoins_num end) num_1000000000
                 ,sum(case when fbank_gamecoins_num > 1000000000 then fbank_gamecoins_num end) num_m1000000000
            from work.bud_all_gamecoin_dis_tmp_%(statdatenum)s t
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,fuser_type;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """
         insert into table bud_dm.bud_user_gamecoin_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,t.fgamefsk
                 ,coalesce(t.fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(t.fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(t.fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(t.fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(t.fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuser_type
                 ,'unum_bank' fdata_type
                 ,count(case when fbank_gamecoins_num = 0 then t.fuid end) num_0
                 ,count(case when fbank_gamecoins_num between 1 and 1000 then t.fuid end) num_1000
                 ,count(case when fbank_gamecoins_num between 1001 and 5000 then t.fuid end) num_5000
                 ,count(case when fbank_gamecoins_num between 5001 and 10000 then t.fuid end) num_10000
                 ,count(case when fbank_gamecoins_num between 10001 and 50000 then t.fuid end) num_50000
                 ,count(case when fbank_gamecoins_num between 50001 and 100000 then t.fuid end) num_100000
                 ,count(case when fbank_gamecoins_num between 100001 and 500000 then t.fuid end) num_500000
                 ,count(case when fbank_gamecoins_num between 500001 and 1000000 then t.fuid end) num_1000000
                 ,count(case when fbank_gamecoins_num between 1000001 and 5000000 then t.fuid end) num_5000000
                 ,count(case when fbank_gamecoins_num between 5000001 and 10000000 then t.fuid end) num_10000000
                 ,count(case when fbank_gamecoins_num between 10000001 and 50000000 then t.fuid end) num_50000000
                 ,count(case when fbank_gamecoins_num between 50000001 and 100000000 then t.fuid end) num_100000000
                 ,count(case when fbank_gamecoins_num between 100000001 and 1000000000 then t.fuid end) num_1000000000
                 ,count(case when fbank_gamecoins_num > 1000000000 then t.fuid end) num_m1000000000
            from work.bud_all_gamecoin_dis_tmp_%(statdatenum)s t
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,fuser_type
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_gamecoin_dis(sys.argv[1:])
a()
