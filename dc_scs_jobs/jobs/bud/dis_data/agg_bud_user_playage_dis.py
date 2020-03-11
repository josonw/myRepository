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


class agg_bud_user_playage_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_playage_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuser_type          varchar(10)      comment '用户类型:act_user',
               num_0               bigint           comment '0',
               num_1               bigint           comment '1',
               num_2               bigint           comment '2-3',
               num_3               bigint           comment '4-7',
               num_4               bigint           comment '8-14',
               num_5               bigint           comment '15-30',
               num_6               bigint           comment '31-60',
               num_7               bigint           comment '61-90',
               num_8               bigint           comment '91-180',
               num_9               bigint           comment '181-365',
               num_10              bigint           comment '365+'
               )comment '用户游戏年龄分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_playage_dis';
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
        hql = """--取所有用户游戏年龄相关指标
            drop table if exists work.bud_user_playage_dis_tmp_1_%(statdatenum)s;
          create table work.bud_user_playage_dis_tmp_1_%(statdatenum)s as
          select  t1.fbpid
                 ,t1.fuid
                 ,datediff('%(statdate)s',dt) fplayage
            from dim.reg_user_main_additional t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取活跃相关指标
            drop table if exists work.bud_user_playage_dis_tmp_2_%(statdatenum)s;
          create table work.bud_user_playage_dis_tmp_2_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,t2.fplayage
            from dim.user_act t1
            left join work.bud_user_playage_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'game_user' fuser_type
                 ,count(distinct case when fplayage = 0 then fuid end) num_0
                 ,count(distinct case when fplayage = 1 then fuid end) num_1
                 ,count(distinct case when fplayage >= 2 and  fplayage <= 3 then fuid end) num_2
                 ,count(distinct case when fplayage >= 4 and  fplayage <= 7 then fuid end) num_3
                 ,count(distinct case when fplayage >= 8 and  fplayage <= 14 then fuid end) num_4
                 ,count(distinct case when fplayage >= 15 and  fplayage <= 30 then fuid end) num_5
                 ,count(distinct case when fplayage >= 31 and  fplayage <= 60 then fuid end) num_6
                 ,count(distinct case when fplayage >= 61 and  fplayage <= 90 then fuid end) num_7
                 ,count(distinct case when fplayage >= 91 and  fplayage <= 180 then fuid end) num_8
                 ,count(distinct case when fplayage >= 181 and  fplayage <= 365 then fuid end) num_9
                 ,count(distinct case when fplayage > 365 then fuid end) num_10
            from work.bud_user_playage_dis_tmp_2_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_user_playage_dis partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取活跃相关指标
            drop table if exists work.bud_user_playage_dis_tmp_3_%(statdatenum)s;
          create table work.bud_user_playage_dis_tmp_3_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,t2.fplayage
            from dim.user_pay_day t1
            left join work.bud_user_playage_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'pay_user' fuser_type
                 ,count(distinct case when fplayage = 0 then fuid end) num_0
                 ,count(distinct case when fplayage = 1 then fuid end) num_1
                 ,count(distinct case when fplayage >= 2 and  fplayage <= 3 then fuid end) num_2
                 ,count(distinct case when fplayage >= 4 and  fplayage <= 7 then fuid end) num_3
                 ,count(distinct case when fplayage >= 8 and  fplayage <= 14 then fuid end) num_4
                 ,count(distinct case when fplayage >= 15 and  fplayage <= 30 then fuid end) num_5
                 ,count(distinct case when fplayage >= 31 and  fplayage <= 60 then fuid end) num_6
                 ,count(distinct case when fplayage >= 61 and  fplayage <= 90 then fuid end) num_7
                 ,count(distinct case when fplayage >= 91 and  fplayage <= 180 then fuid end) num_8
                 ,count(distinct case when fplayage >= 181 and  fplayage <= 365 then fuid end) num_9
                 ,count(distinct case when fplayage > 365 then fuid end) num_10
            from work.bud_user_playage_dis_tmp_3_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert into table bud_dm.bud_user_playage_dis partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_playage_dis_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_playage_dis_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_user_playage_dis_tmp_3_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_playage_dis(sys.argv[1:])
a()
