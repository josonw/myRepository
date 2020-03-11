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


class agg_bud_user_match_weekly_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_match_weekly_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fjoin_unum          bigint     comment '报名人数',
               fmatch_unum         bigint     comment '参赛人数'
               )comment '赛事用户信息周表'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_match_weekly_info';
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

        # 取基础数据
        hql = """--取活跃相关指标
            drop table if exists work.bud_user_match_weekly_info_tmp_%(statdatenum)s;
          create table work.bud_user_match_weekly_info_tmp_%(statdatenum)s as
          select distinct  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fuid
                 ,1 flag
            from dim.match_gameparty t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt <= '%(statdate)s'
             and t1.dt >= '%(ld_week_begin)s'

           union all

          select distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fuid
                 ,2 flag
            from dim.join_gameparty t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt <= '%(statdate)s'
             and t1.dt >= '%(ld_week_begin)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总  --子游戏
        hql = """
          select "%(ld_week_begin)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,count(distinct case when flag = 2 then fuid end) fjoin_unum               --报名人数
                 ,count(distinct case when flag = 1 then fuid end) fmatch_unum              --参赛人数
            from work.bud_user_match_weekly_info_tmp_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """
        self.sql_template_build(sql=hql)

        # 组合
        hql = """insert overwrite table bud_dm.bud_user_match_weekly_info
                      partition(dt='%(ld_week_begin)s')
                      %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_match_weekly_info_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_match_weekly_info(sys.argv[1:])
a()
