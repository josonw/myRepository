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


class agg_bud_user_act_hour_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_act_hour_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fhourfsk            bigint        comment '时段',
               fact_unum           bigint        comment '活跃用户',
               fplay_unum          bigint        comment '玩牌用户',
               fgcoin_unum         bigint        comment '金流用户',
               flogin_unum         bigint        comment '登录用户',
               flogin_num          bigint        comment '登录次数'
               )comment '分时段用户活跃'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_act_hour_dis';
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
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fhourfsk),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fhourfsk),
                               (fgamefsk, fgame_id, fhourfsk) )
                         union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fhourfsk),
                               (fgamefsk, fplatformfsk, fhallfsk, fhourfsk),
                               (fgamefsk, fplatformfsk, fhourfsk) ) """

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
        hql = """--取子游戏新增相关指标
            drop table if exists work.bud_user_act_hour_dis_tmp_1_%(statdatenum)s;
          create table work.bud_user_act_hour_dis_tmp_1_%(statdatenum)s as
          --子游戏登录
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,hour(t1.flts_at)+1 fhourfsk
                 ,t1.fuid
                 ,1 type
            from stage.user_enter_stg t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          --大厅登录
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,hour(t1.flogin_at)+1 fhourfsk
                 ,t1.fuid
                 ,1 type
            from dim.user_login_additional t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          --玩牌
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,hour(t1.flts_at)+1 fhourfsk
                 ,t1.fuid
                 ,2 type
            from stage.user_gameparty_stg t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          --金流
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,hour(t1.lts_at)+1 fhourfsk
                 ,t1.fuid
                 ,3 type
            from stage.pb_gamecoins_stream_stg t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fhourfsk
                 ,count(distinct fuid) fact_unum       --活跃用户
                 ,count(distinct case when type = 2 then fuid end) fplay_unum      --玩牌用户
                 ,count(distinct case when type = 3 then fuid end) fgcoin_unum     --金流用户
                 ,count(distinct case when type = 1 then fuid end) flogin_unum     --登录用户
                 ,count(case when type = 1 then fuid end) flogin_num      --登录次数
            from work.bud_user_act_hour_dis_tmp_1_%(statdatenum)s t1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fhourfsk
         """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_user_act_hour_dis
            partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_act_hour_dis_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_act_hour_dis(sys.argv[1:])
a()
