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


class agg_bud_user_act_weekly_detail(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_act_weekly_detail (
       fdate               date,
       fgamefsk            bigint,
       fplatformfsk        bigint,
       fhallfsk            bigint,
       fsubgamefsk         bigint,
       fterminaltypefsk    bigint,
       fversionfsk         bigint,
       fact_unum           bigint     comment '活跃人数',
       fnew_unum           bigint     comment '本周新用户：周活跃用户中本周注册的的用户数',
       fref_unum           bigint     comment '本周回流用户：本周活跃，但上周并未活跃过的老用户',
       f2w_act_unum        bigint     comment '连续活跃2周用户：本周以及上周均有活跃行为，但上上周无活跃行为的老用户',
       f3w_act_unum        bigint     comment '连续活跃3周用户',
       f4w_act_unum        bigint     comment '连续活跃4周用户',
       floyal_unum         bigint     comment '忠诚用户：连续活跃5周及以上的用户'
       )comment '活跃用户信息周表'
       partitioned by(dt string)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        query = {'statdatenum': """%(statdatenum)s""",
                 'statdate': """%(statdate)s"""}

        dates_dict = PublicFunc.date_define(self.stat_date)
        query.update(dates_dict)

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = ("""
          drop table if exists work.bud_user_act_weekly_detail_tmp_1_%(statdatenum)s;
        create table work.bud_user_act_weekly_detail_tmp_1_%(statdatenum)s as
          select  t.fgamefsk
                 ,t.fplatformfsk
                 ,t.fhallfsk
                 ,t.fgame_id
                 ,t.fterminaltypefsk
                 ,t.fversionfsk
                 ,t.fuid
                 ,case when t1.fuid is not null then 1 else 0 end act_week_2
                 ,case when t2.fuid is not null then 1 else 0 end act_week_3
                 ,case when t3.fuid is not null then 1 else 0 end act_week_4
                 ,case when t4.fuid is not null then 1 else 0 end act_week_5
                 ,case when t5.fuid is not null then 1 else 0 end act_new
            from dim.user_act_week_array t
            left join dim.user_act_week_array t1
              on t.fuid = t1.fuid
             and t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fgame_id = t1.fgame_id
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t1.dt = '%(ld_1weekago_begin)s'
            left join dim.user_act_week_array t2
              on t.fuid = t2.fuid
             and t.fgamefsk = t2.fgamefsk
             and t.fplatformfsk = t2.fplatformfsk
             and t.fhallfsk = t2.fhallfsk
             and t.fgame_id = t2.fgame_id
             and t.fterminaltypefsk = t2.fterminaltypefsk
             and t.fversionfsk = t2.fversionfsk
             and t2.dt = '%(ld_2weekago_begin)s'
            left join dim.user_act_week_array t3
              on t.fuid = t3.fuid
             and t.fgamefsk = t3.fgamefsk
             and t.fplatformfsk = t3.fplatformfsk
             and t.fhallfsk = t3.fhallfsk
             and t.fgame_id = t3.fgame_id
             and t.fterminaltypefsk = t3.fterminaltypefsk
             and t.fversionfsk = t3.fversionfsk
             and t3.dt = '%(ld_3weekago_begin)s'
            left join dim.user_act_week_array t4
              on t.fuid = t4.fuid
             and t.fgamefsk = t4.fgamefsk
             and t.fplatformfsk = t4.fplatformfsk
             and t.fhallfsk = t4.fhallfsk
             and t.fgame_id = t4.fgame_id
             and t.fterminaltypefsk = t4.fterminaltypefsk
             and t.fversionfsk = t4.fversionfsk
             and t4.dt = '%(ld_4weekago_begin)s'
            left join dim.reg_user_array t5
              on t.fuid = t5.fuid
             and t.fgamefsk = t5.fgamefsk
             and t.fplatformfsk = t5.fplatformfsk
             and t.fhallfsk = t5.fhallfsk
             and t.fgame_id = t5.fgame_id
             and t.fterminaltypefsk = t5.fterminaltypefsk
             and t.fversionfsk = t5.fversionfsk
             and t5.dt >= '%(ld_weekbegin)s'
             and t5.dt <= '%(ld_weekend)s'
           where t.dt = '%(ld_weekbegin)s'
         """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 组合
        hql = ("""insert overwrite table bud_dm.bud_user_act_weekly_detail
            partition(dt='%(ld_weekbegin)s')
            select '%(ld_weekbegin)s' fdate
                   ,fgamefsk
                   ,fplatformfsk
                   ,fhallfsk
                   ,fgame_id fsubgamefsk
                   ,fterminaltypefsk
                   ,fversionfsk
                   ,count(distinct fuid) fact_unum
                   ,count(distinct case when act_new = 1 then fuid end) fnew_unum
                   ,count(distinct case when act_new = 0 and act_week_2 = 0 then fuid end) fref_unum
                   ,count(distinct case when act_new = 0 and act_week_2 = 1 and act_week_3 = 0 then fuid end) f2w_act_unum
                   ,count(distinct case when act_new = 0 and act_week_2 = 1 and act_week_3 = 1 and act_week_4 = 0 then fuid end) f3w_act_unum
                   ,count(distinct case when act_new = 0 and act_week_2 = 1 and act_week_3 = 1 and act_week_4 = 1 and act_week_5 = 0 then fuid end) f4w_act_unum
                   ,count(distinct case when act_new = 0 and act_week_2 = 1 and act_week_3 = 1 and act_week_4 = 1 and act_week_5 = 1 then fuid end) floyal_unum
              from work.bud_user_act_weekly_detail_tmp_1_%(statdatenum)s t
             group by fgamefsk
                      ,fplatformfsk
                      ,fhallfsk
                      ,fgame_id
                      ,fterminaltypefsk
                      ,fversionfsk""") % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_act_weekly_detail_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_act_weekly_detail(sys.argv[1:])
a()
