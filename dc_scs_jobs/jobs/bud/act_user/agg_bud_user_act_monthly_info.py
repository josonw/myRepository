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


class agg_bud_user_act_monthly_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_act_monthly_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fact_unum           bigint     comment '活跃人数',
               fact_log_unum       bigint     comment '登录人数',
               fact_play_unum      bigint     comment '玩牌活跃人数',
               fact_coin_unum      bigint     comment '金流活跃人数'
               )comment '活跃用户信息月表'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_act_monthly_info';

        create table if not exists bud_dm.bud_user_act_weekly_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fact_unum           bigint     comment '活跃人数',
               fact_log_unum       bigint     comment '登录人数',
               fact_play_unum      bigint     comment '玩牌活跃人数',
               fact_coin_unum      bigint     comment '金流活跃人数'
               )comment '活跃用户信息周表'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_act_weekly_info';
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
                               (fgamefsk, fgame_id) )"""

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk),
                               (fgamefsk, fplatformfsk, fhallfsk),
                               (fgamefsk, fplatformfsk) )"""

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
        hql = """--取活跃相关指标
            drop table if exists work.bud_user_act_monthly_info_tmp_%(statdatenum)s;
          create table work.bud_user_act_monthly_info_tmp_%(statdatenum)s as
          select distinct t2.fgamefsk
                 ,t2.fplatformfsk
                 ,t2.fhallfsk
                 ,t1.fgame_id
                 ,t2.fterminaltypefsk
                 ,t2.fversionfsk
                 ,t1.fuid
                 ,t1.flogin_cnt             --登陆次数
                 ,t1.fparty_num             --玩牌局数
                 ,t1.fis_change_gamecoins   --金流是否发生变化
                 ,case when t1.dt > '%(ld_7day_ago)s' then 1 else 0 end is_week
            from dim.user_act t1
            join dim.bpid_map_bud t2
              on t1.fbpid = t2.fbpid
           where t1.dt <= '%(statdate)s'
             and t1.dt >= '%(ld_30day_ago)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总  --子游戏
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,count(distinct fuid) fact_unum                                                          --活跃人数
                 ,count(distinct case when flogin_cnt > 0 then fuid end) fact_log_unum                    --登录人数
                 ,count(distinct case when fparty_num > 0 then fuid end) fact_play_unum                   --玩牌活跃人数
                 ,count(distinct case when fis_change_gamecoins > 0 then fuid end) fact_coin_unum         --金流活跃人数
            from work.bud_user_act_monthly_info_tmp_%(statdatenum)s t
           where t.fgame_id <> -13658
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_user_act_monthly_info
            partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总  --子游戏
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,count(distinct fuid) fact_unum                                                          --活跃人数
                 ,count(distinct case when flogin_cnt > 0 then fuid end) fact_log_unum                    --登录人数
                 ,count(distinct case when fparty_num > 0 then fuid end) fact_play_unum                   --玩牌活跃人数
                 ,count(distinct case when fis_change_gamecoins > 0 then fuid end) fact_coin_unum         --金流活跃人数
            from work.bud_user_act_monthly_info_tmp_%(statdatenum)s t
           where t.fgame_id <> -13658 and is_week = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_user_act_weekly_info
            partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总  --非子游戏
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,%(null_int_group_rule)d fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,count(distinct fuid) fact_unum                                                          --活跃人数
                 ,count(distinct case when flogin_cnt > 0 then fuid end) fact_log_unum                    --登录人数
                 ,count(distinct case when fparty_num > 0 then fuid end) fact_play_unum                   --玩牌活跃人数
                 ,count(distinct case when fis_change_gamecoins > 0 then fuid end) fact_coin_unum         --金流活跃人数
            from work.bud_user_act_monthly_info_tmp_%(statdatenum)s t
           where t.fgame_id = -13658
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert into table bud_dm.bud_user_act_monthly_info
            partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总  --非子游戏
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,%(null_int_group_rule)d fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,count(distinct fuid) fact_unum                                                          --活跃人数
                 ,count(distinct case when flogin_cnt > 0 then fuid end) fact_log_unum                    --登录人数
                 ,count(distinct case when fparty_num > 0 then fuid end) fact_play_unum                   --玩牌活跃人数
                 ,count(distinct case when fis_change_gamecoins > 0 then fuid end) fact_coin_unum         --金流活跃人数
            from work.bud_user_act_monthly_info_tmp_%(statdatenum)s t
           where t.fgame_id = -13658 and is_week = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert into table bud_dm.bud_user_act_weekly_info
            partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_act_monthly_info_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_act_monthly_info(sys.argv[1:])
a()