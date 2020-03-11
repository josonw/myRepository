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


class agg_bud_all_gamecoin_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_all_gamecoin_dis (
               fdate                   date,
               fgamefsk                bigint,
               fplatformfsk            bigint,
               fhallfsk                bigint,
               fsubgamefsk             bigint,
               fterminaltypefsk        bigint,
               fversionfsk             bigint,
               fchannelcode            bigint,
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
               flft                    bigint        comment '区间下界',
               frgt                    bigint        comment '区间上界',
               fpaid_unum              bigint        comment '历史付费活跃用户数',
               fpaid_num               bigint        comment '历史付费活跃货币数'
               )comment '保险箱加携带游戏币分布'
               partitioned by(dt string)
        location '/dw/bud_dm/bud_all_gamecoin_dis';

        create table if not exists bud_dm.bud_bring_gamecoin_dis (
               fdate                   date,
               fgamefsk                bigint,
               fplatformfsk            bigint,
               fhallfsk                bigint,
               fsubgamefsk             bigint,
               fterminaltypefsk        bigint,
               fversionfsk             bigint,
               fchannelcode            bigint,
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
               flft                    bigint        comment '区间下界',
               frgt                    bigint        comment '区间上界',
               fpaid_unum              bigint        comment '历史付费活跃用户数',
               fpaid_num               bigint        comment '历史付费活跃货币数'
               )comment '携带游戏币分布'
               partitioned by(dt string)
        location '/dw/bud_dm/bud_bring_gamecoin_dis';

        create table if not exists bud_dm.bud_bank_gamecoin_dis (
               fdate                   date,
               fgamefsk                bigint,
               fplatformfsk            bigint,
               fhallfsk                bigint,
               fsubgamefsk             bigint,
               fterminaltypefsk        bigint,
               fversionfsk             bigint,
               fchannelcode            bigint,
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
               flft                    bigint        comment '区间下界',
               frgt                    bigint        comment '区间上界',
               fpaid_unum              bigint        comment '历史付费活跃用户数',
               fpaid_num               bigint        comment '历史付费活跃货币数'
               )comment '保险箱游戏币分布'
               partitioned by(dt string)
        location '/dw/bud_dm/bud_bank_gamecoin_dis';
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
         insert overwrite table bud_dm.bud_all_gamecoin_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fgame_id
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,-21379 fchannelcode
                 ,count(case when fuser_type = 'act_user' then fuid end) fact_unum   --活跃用户数
                 ,sum(case when fuser_type = 'act_user' then coalesce(t.fbank_gamecoins_num, 0)+ coalesce(t.fgamecoins_num, 0) end) fact_num   --活跃货币数
                 ,count(case when fuser_type = 'reg_user' then fuid end) freg_unum   --新增用户数
                 ,sum(case when fuser_type = 'reg_user' then coalesce(t.fbank_gamecoins_num, 0)+ coalesce(t.fgamecoins_num, 0) end) freg_num   --新增货币数
                 ,count(case when fuser_type = 'pay_user' then fuid end) fpay_unum   --付费用户数
                 ,sum(case when fuser_type = 'pay_user' then coalesce(t.fbank_gamecoins_num, 0)+ coalesce(t.fgamecoins_num, 0) end) fpay_num   --付费货币数
                 ,count(case when fuser_type = 'rupt_user' then fuid end) fbankrupt_unum   --破产用户数
                 ,sum(case when fuser_type = 'rupt_user' then coalesce(t.fbank_gamecoins_num, 0)+ coalesce(t.fgamecoins_num, 0) end) fbankrupt_num   --破产货币数
                 ,count(case when fuser_type = 'all_user' then fuid end) fall_unum   --所有用户数
                 ,sum(case when fuser_type = 'all_user' then coalesce(t.fbank_gamecoins_num, 0)+ coalesce(t.fgamecoins_num, 0) end) fall_num   --所有货币数
                 ,all_flft flft   --区间下界
                 ,all_flft frgt   --区间上界
                 ,count(case when fuser_type = 'paid_user' then fuid end) fpaid_unum   --历史付费活跃用户数
                 ,sum(case when fuser_type = 'paid_user' then coalesce(t.fbank_gamecoins_num, 0)+ coalesce(t.fgamecoins_num, 0) end) fpaid_num   --历史付费活跃货币数
            from work.bud_all_gamecoin_dis_tmp_%(statdatenum)s t
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,all_flft
                    ,all_flft;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --总金额
        hql = """
         insert overwrite table bud_dm.bud_bring_gamecoin_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fgame_id
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,-21379 fchannelcode
                 ,count(case when fuser_type = 'act_user' then fuid end) fact_unum   --活跃用户数
                 ,sum(case when fuser_type = 'act_user' then coalesce(t.fgamecoins_num, 0) end) fact_num   --活跃货币数
                 ,count(case when fuser_type = 'reg_user' then fuid end) freg_unum   --新增用户数
                 ,sum(case when fuser_type = 'reg_user' then coalesce(t.fgamecoins_num, 0) end) freg_num   --新增货币数
                 ,count(case when fuser_type = 'pay_user' then fuid end) fpay_unum   --付费用户数
                 ,sum(case when fuser_type = 'pay_user' then coalesce(t.fgamecoins_num, 0) end) fpay_num   --付费货币数
                 ,count(case when fuser_type = 'rupt_user' then fuid end) fbankrupt_unum   --破产用户数
                 ,sum(case when fuser_type = 'rupt_user' then coalesce(t.fgamecoins_num, 0) end) fbankrupt_num   --破产货币数
                 ,count(case when fuser_type = 'all_user' then fuid end) fall_unum   --所有用户数
                 ,sum(case when fuser_type = 'all_user' then coalesce(t.fgamecoins_num, 0) end) fall_num   --所有货币数
                 ,br_flft flft   --区间下界
                 ,br_flft frgt   --区间上界
                 ,count(case when fuser_type = 'paid_user' then fuid end) fpaid_unum   --历史付费活跃用户数
                 ,sum(case when fuser_type = 'paid_user' then coalesce(t.fgamecoins_num, 0) end) fpaid_num   --历史付费活跃货币数
            from work.bud_all_gamecoin_dis_tmp_%(statdatenum)s t
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,br_flft
                    ,br_flft;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --总金额
        hql = """
         insert overwrite table bud_dm.bud_bank_gamecoin_dis
         partition( dt="%(statdate)s" )
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fgame_id
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,-21379 fchannelcode
                 ,count(case when fuser_type = 'act_user' then fuid end) fact_unum   --活跃用户数
                 ,sum(case when fuser_type = 'act_user' then coalesce(t.fbank_gamecoins_num, 0) end) fact_num   --活跃货币数
                 ,count(case when fuser_type = 'reg_user' then fuid end) freg_unum   --新增用户数
                 ,sum(case when fuser_type = 'reg_user' then coalesce(t.fbank_gamecoins_num, 0) end) freg_num   --新增货币数
                 ,count(case when fuser_type = 'pay_user' then fuid end) fpay_unum   --付费用户数
                 ,sum(case when fuser_type = 'pay_user' then coalesce(t.fbank_gamecoins_num, 0) end) fpay_num   --付费货币数
                 ,count(case when fuser_type = 'rupt_user' then fuid end) fbankrupt_unum   --破产用户数
                 ,sum(case when fuser_type = 'rupt_user' then coalesce(t.fbank_gamecoins_num, 0) end) fbankrupt_num   --破产货币数
                 ,count(case when fuser_type = 'all_user' then fuid end) fall_unum   --所有用户数
                 ,sum(case when fuser_type = 'all_user' then coalesce(t.fbank_gamecoins_num, 0) end) fall_num   --所有货币数
                 ,ba_flft flft   --区间下界
                 ,ba_flft frgt   --区间上界
                 ,count(case when fuser_type = 'paid_user' then fuid end) fpaid_unum   --历史付费活跃用户数
                 ,sum(case when fuser_type = 'paid_user' then coalesce(t.fbank_gamecoins_num, 0) end) fpaid_num   --历史付费活跃货币数
            from work.bud_all_gamecoin_dis_tmp_%(statdatenum)s t
           group by t.fgamefsk,t.fplatformfsk,t.fhallfsk,t.fgame_id,t.fterminaltypefsk,t.fversionfsk
                    ,ba_flft
                    ,ba_flft;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_all_gamecoin_dis_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_all_gamecoin_dis_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_all_gamecoin_dis(sys.argv[1:])
a()
