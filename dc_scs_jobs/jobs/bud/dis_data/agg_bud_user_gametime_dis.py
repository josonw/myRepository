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


class agg_bud_user_gametime_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_gametime_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuser_type          varchar(10)      comment '用户类型:reg_user,act_user',
               num_0               bigint           comment '0',
               num_5               bigint           comment '(0,5]',
               num_10              bigint           comment '(5,10]',
               num_15              bigint           comment '(10,15]',
               num_20              bigint           comment '(15,20]',
               num_30              bigint           comment '(20,30]',
               num_40              bigint           comment '(30,40]',
               num_50              bigint           comment '(40,50]',
               num_60              bigint           comment '(50,60]',
               num_90              bigint           comment '(60,90]',
               num_120             bigint           comment '(90,120]',
               num_180             bigint           comment '(120,180]',
               num_m180            bigint           comment '>180'
               )comment '用户玩牌时长分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_gametime_dis';
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

        extend_group_3 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id),
                               (fgamefsk, fgame_id) )  """

        extend_group_4 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk),
                               (fgamefsk, fplatformfsk, fhallfsk),
                               (fgamefsk, fplatformfsk) ) """

        query = {'extend_group_1': extend_group_1,
                 'extend_group_2': extend_group_2,
                 'extend_group_3': extend_group_3,
                 'extend_group_4': extend_group_4,
                 'null_str_group_rule': sql_const.NULL_STR_GROUP_RULE,
                 'null_int_group_rule': sql_const.NULL_INT_GROUP_RULE,
                 'statdatenum': """%(statdatenum)s""",
                 'statdate': """%(statdate)s"""}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--取注册相关指标
            drop table if exists work.bud_user_gametime_dis_tmp_1_%(statdatenum)s;
          create table work.bud_user_gametime_dis_tmp_1_%(statdatenum)s as
          select  t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,t1.fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,t1.fuid
                 ,coalesce(sum(fplay_time),0) fplay_time
            from dim.reg_user_array t1
            left join dim.user_gameparty_array t2
              on t1.fuid = t2.fuid
             and t1.fgamefsk = t2.fgamefsk
             and t1.fplatformfsk = t2.fplatformfsk
             and t1.fhallfsk = t2.fhallfsk
             and t1.fgame_id = t2.fsubgamefsk
             and t1.fterminaltypefsk = t2.fterminaltypefsk
             and t1.fversionfsk = t2.fversionfsk
             and t1.fchannel_code = t2.fchannelcode
             and t2.dt = '%(statdate)s'
           where t1.dt='%(statdate)s'
           group by t1.fgamefsk
                    ,t1.fplatformfsk
                    ,t1.fhallfsk
                    ,t1.fgame_id
                    ,t1.fterminaltypefsk
                    ,t1.fversionfsk
                    ,t1.fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg1
        hql = """
        insert overwrite table bud_dm.bud_user_gametime_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'reg_user' fuser_type
                 ,count(distinct case when fplay_time = 0 then fuid end) num_0
                 ,count(distinct case when fplay_time/60 > 0 and fplay_time/60 <= 5 then fuid end) num_5
                 ,count(distinct case when fplay_time/60 > 5 and fplay_time/60 <= 10 then fuid end) num_10
                 ,count(distinct case when fplay_time/60 > 10 and fplay_time/60 <= 15 then fuid end) num_15
                 ,count(distinct case when fplay_time/60 > 15 and fplay_time/60 <= 20 then fuid end) num_20
                 ,count(distinct case when fplay_time/60 > 20 and fplay_time/60 <= 30 then fuid end) num_30
                 ,count(distinct case when fplay_time/60 > 30 and fplay_time/60 <= 40 then fuid end) num_40
                 ,count(distinct case when fplay_time/60 > 40 and fplay_time/60 <= 50 then fuid end) num_50
                 ,count(distinct case when fplay_time/60 > 50 and fplay_time/60 <= 60 then fuid end) num_60
                 ,count(distinct case when fplay_time/60 > 60 and fplay_time/60 <= 90 then fuid end) num_90
                 ,count(distinct case when fplay_time/60 > 90 and fplay_time/60 <= 120 then fuid end) num_120
                 ,count(distinct case when fplay_time/60 > 120 and fplay_time/60 <= 180 then fuid end) num_180
                 ,count(distinct case when fplay_time/60 > 180 then fuid end) num_m180
            from work.bud_user_gametime_dis_tmp_1_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取活跃相关指标
            drop table if exists work.bud_user_gametime_dis_tmp_3_%(statdatenum)s;
          create table work.bud_user_gametime_dis_tmp_3_%(statdatenum)s as
          select  t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,t1.fsubgamefsk fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,t1.fuid
                 ,coalesce(sum(fplay_time),0) fplay_time
            from dim.user_act_array t1
            left join dim.user_gameparty_array t2
              on t1.fuid = t2.fuid
             and t1.fgamefsk = t2.fgamefsk
             and t1.fplatformfsk = t2.fplatformfsk
             and t1.fhallfsk = t2.fhallfsk
             and t1.fsubgamefsk = t2.fsubgamefsk
             and t1.fterminaltypefsk = t2.fterminaltypefsk
             and t1.fversionfsk = t2.fversionfsk
             and t1.fchannelcode = t2.fchannelcode
             and t2.dt = '%(statdate)s'
           where t1.dt='%(statdate)s'
           group by t1.fgamefsk
                    ,t1.fplatformfsk
                    ,t1.fhallfsk
                    ,t1.fsubgamefsk
                    ,t1.fterminaltypefsk
                    ,t1.fversionfsk
                    ,t1.fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总act1
        hql = """
        insert into table bud_dm.bud_user_gametime_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'act_user' fuser_type
                 ,count(distinct case when fplay_time = 0 then fuid end) num_0
                 ,count(distinct case when fplay_time/60 > 0 and fplay_time/60 <= 5 then fuid end) num_5
                 ,count(distinct case when fplay_time/60 > 5 and fplay_time/60 <= 10 then fuid end) num_10
                 ,count(distinct case when fplay_time/60 > 10 and fplay_time/60 <= 15 then fuid end) num_15
                 ,count(distinct case when fplay_time/60 > 15 and fplay_time/60 <= 20 then fuid end) num_20
                 ,count(distinct case when fplay_time/60 > 20 and fplay_time/60 <= 30 then fuid end) num_30
                 ,count(distinct case when fplay_time/60 > 30 and fplay_time/60 <= 40 then fuid end) num_40
                 ,count(distinct case when fplay_time/60 > 40 and fplay_time/60 <= 50 then fuid end) num_50
                 ,count(distinct case when fplay_time/60 > 50 and fplay_time/60 <= 60 then fuid end) num_60
                 ,count(distinct case when fplay_time/60 > 60 and fplay_time/60 <= 90 then fuid end) num_90
                 ,count(distinct case when fplay_time/60 > 90 and fplay_time/60 <= 120 then fuid end) num_120
                 ,count(distinct case when fplay_time/60 > 120 and fplay_time/60 <= 180 then fuid end) num_180
                 ,count(distinct case when fplay_time/60 > 180 then fuid end) num_m180
            from work.bud_user_gametime_dis_tmp_3_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取付费相关指标
            drop table if exists work.bud_user_gametime_dis_tmp_4_%(statdatenum)s;
          create table work.bud_user_gametime_dis_tmp_4_%(statdatenum)s as
          select  t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,t1.fsubgamefsk fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,t1.fuid
                 ,coalesce(sum(fplay_time),0) fplay_time
            from dim.user_pay_array t1
            left join dim.user_gameparty_array t2
              on t1.fuid = t2.fuid
             and t1.fgamefsk = t2.fgamefsk
             and t1.fplatformfsk = t2.fplatformfsk
             and t1.fhallfsk = t2.fhallfsk
             and t1.fsubgamefsk = t2.fsubgamefsk
             and t1.fterminaltypefsk = t2.fterminaltypefsk
             and t1.fversionfsk = t2.fversionfsk
             and t1.fchannelcode = t2.fchannelcode
             and t2.dt = '%(statdate)s'
           where t1.dt='%(statdate)s'
           group by t1.fgamefsk
                    ,t1.fplatformfsk
                    ,t1.fhallfsk
                    ,t1.fsubgamefsk
                    ,t1.fterminaltypefsk
                    ,t1.fversionfsk
                    ,t1.fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总pay
        hql = """
        insert into table bud_dm.bud_user_gametime_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'pay_user' fuser_type
                 ,count(distinct case when fplay_time = 0 then fuid end) num_0
                 ,count(distinct case when fplay_time/60 > 0 and fplay_time/60 <= 5 then fuid end) num_5
                 ,count(distinct case when fplay_time/60 > 5 and fplay_time/60 <= 10 then fuid end) num_10
                 ,count(distinct case when fplay_time/60 > 10 and fplay_time/60 <= 15 then fuid end) num_15
                 ,count(distinct case when fplay_time/60 > 15 and fplay_time/60 <= 20 then fuid end) num_20
                 ,count(distinct case when fplay_time/60 > 20 and fplay_time/60 <= 30 then fuid end) num_30
                 ,count(distinct case when fplay_time/60 > 30 and fplay_time/60 <= 40 then fuid end) num_40
                 ,count(distinct case when fplay_time/60 > 40 and fplay_time/60 <= 50 then fuid end) num_50
                 ,count(distinct case when fplay_time/60 > 50 and fplay_time/60 <= 60 then fuid end) num_60
                 ,count(distinct case when fplay_time/60 > 60 and fplay_time/60 <= 90 then fuid end) num_90
                 ,count(distinct case when fplay_time/60 > 90 and fplay_time/60 <= 120 then fuid end) num_120
                 ,count(distinct case when fplay_time/60 > 120 and fplay_time/60 <= 180 then fuid end) num_180
                 ,count(distinct case when fplay_time/60 > 180 then fuid end) num_m180
            from work.bud_user_gametime_dis_tmp_4_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_gametime_dis_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_gametime_dis_tmp_3_%(statdatenum)s;
                 drop table if exists work.bud_user_gametime_dis_tmp_4_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_gametime_dis(sys.argv[1:])
a()
