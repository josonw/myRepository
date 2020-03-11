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


class agg_bud_game_activity_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_game_activity_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fact_id             varchar(50),
               fact_name           varchar(100),
               fclick_unum         bigint,
               fclick_cnt          bigint
               )comment '游戏活动概要数据'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_game_activity_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fact_id', 'fact_name'],
                        'groups': [[1, 1],
                                   [0, 0]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--取玩牌相关指标
            drop table if exists work.bud_game_activity_info_tmp_1_%(statdatenum)s;
          create table work.bud_game_activity_info_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.hallmode
                 ,tt.fversionfsk
                 ,t1.fact_id
                 ,t1.fact_name
                 ,t1.fuid
            from stage.activity_expose_stg t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt='%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,coalesce(fact_id,'%(null_str_group_rule)s') fact_id
                 ,coalesce(fact_name,'%(null_str_group_rule)s') fact_name
                 ,count(distinct fuid) fclick_unum                                    --活动点击人数
                 ,count(fuid) fclick_cnt                                            --活动点击人次
            from work.bud_game_activity_info_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fact_id,fact_name
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """insert overwrite table bud_dm.bud_game_activity_info  partition(dt='%(statdate)s')
              %(sql_template)s
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_game_activity_info_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_game_activity_info(sys.argv[1:])
a()
