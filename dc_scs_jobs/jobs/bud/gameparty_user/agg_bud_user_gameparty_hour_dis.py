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


class agg_bud_user_gameparty_hour_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_gameparty_hour_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fhourfsk            bigint         comment '时段',
               fplay_unum          bigint         comment '玩牌人数',
               fuser_num           bigint         comment '玩牌人次',
               fparty_num          bigint         comment '牌局数',
               fcharge             decimal(30,2)  comment '台费'
               )comment '分时段牌局'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_gameparty_hour_dis';
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
        hql = """--取玩牌相关指标
            drop table if exists work.bud_user_gameparty_hour_dis_tmp_1_%(statdatenum)s;
          create table work.bud_user_gameparty_hour_dis_tmp_1_%(statdatenum)s as
          select  t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,t1.fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,hour(t1.flts_at)+1 fhourfsk
                 ,t1.fuid
                 ,t1.ftbl_id
                 ,t1.finning_id
                 ,t1.fcharge
            from dim.user_gameparty_stream t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s' ;
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
                 ,count(distinct t1.fuid) fplay_unum
                 ,count(1) fuser_num
                 ,count(distinct concat_ws('0', t1.ftbl_id, t1.finning_id) ) fparty_num
                 ,sum(t1.fcharge) fcharge
            from work.bud_user_gameparty_hour_dis_tmp_1_%(statdatenum)s t1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fhourfsk
         """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_user_gameparty_hour_dis
            partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_gameparty_hour_dis_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_gameparty_hour_dis(sys.argv[1:])
a()
