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


class agg_bud_user_act_monthly_detail(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_act_monthly_detail (
       fdate               date,
       fgamefsk            bigint,
       fplatformfsk        bigint,
       fhallfsk            bigint,
       fsubgamefsk         bigint,
       fterminaltypefsk    bigint,
       fversionfsk         bigint,
       fact_unum           bigint     comment '本月活跃人数',
       floss_new_unum      bigint     comment '流失新用户：上个周期新用户中本周期没有回访的用户量',
       floss_old_unum      bigint     comment '流失老用户：上个周期老用户中本周期没有回访的用户量',
       fret_unum           bigint     comment '留存用户：上个周期用户中本周期又回访的用户量',
       fref_unum           bigint     comment '回流用户：老用户中上个周期没有访问，本周期又回访的用户量',
       fnew_unum           bigint     comment '新增用户：本周期首次访问用户量'
       )comment '活跃用户信息月表'
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

        hql = ("""
          drop table if exists work.bud_user_act_monthly_detail_tmp_1_%(statdatenum)s;
        create table work.bud_user_act_monthly_detail_tmp_1_%(statdatenum)s as
          select  t.fgamefsk
                 ,t.fplatformfsk
                 ,t.fhallfsk
                 ,t.fgame_id
                 ,t.fterminaltypefsk
                 ,t.fversionfsk
                 ,t.fuid
                 ,case when t1.fuid is not null then 1 else 0 end act_mon_2
                 ,case when t5.fuid is not null then 1 else 0 end act_new
            from dim.user_act_month_array t
            left join dim.user_act_month_array t1
              on t.fuid = t1.fuid
             and t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fgame_id = t1.fgame_id
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t1.dt = '%(ld_1monthago_begin)s'
            left join dim.reg_user_array t5
              on t.fuid = t5.fuid
             and t.fgamefsk = t5.fgamefsk
             and t.fplatformfsk = t5.fplatformfsk
             and t.fhallfsk = t5.fhallfsk
             and t.fgame_id = t5.fgame_id
             and t.fterminaltypefsk = t5.fterminaltypefsk
             and t.fversionfsk = t5.fversionfsk
             and t5.dt >= '%(ld_monthbegin)s'
             and t5.dt < '%(ld_monthend)s'
           where t.dt = '%(ld_monthbegin)s'
         """) % query
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = ("""
          drop table if exists work.bud_user_act_monthly_detail_tmp_2_%(statdatenum)s;
        create table work.bud_user_act_monthly_detail_tmp_2_%(statdatenum)s as
          select  t.fgamefsk
                 ,t.fplatformfsk
                 ,t.fhallfsk
                 ,t.fgame_id
                 ,t.fterminaltypefsk
                 ,t.fversionfsk
                 ,t.fuid
                 ,case when t5.fuid is not null then 1 else 0 end act_new
            from dim.user_act_month_array t
            left join dim.user_act_month_array t1
              on t.fuid = t1.fuid
             and t.fgamefsk = t1.fgamefsk
             and t.fplatformfsk = t1.fplatformfsk
             and t.fhallfsk = t1.fhallfsk
             and t.fgame_id = t1.fgame_id
             and t.fterminaltypefsk = t1.fterminaltypefsk
             and t.fversionfsk = t1.fversionfsk
             and t1.dt = '%(ld_monthbegin)s'
            left join dim.reg_user_array t5
              on t.fuid = t5.fuid
             and t.fgamefsk = t5.fgamefsk
             and t.fplatformfsk = t5.fplatformfsk
             and t.fhallfsk = t5.fhallfsk
             and t.fgame_id = t5.fgame_id
             and t.fterminaltypefsk = t5.fterminaltypefsk
             and t.fversionfsk = t5.fversionfsk
             and t5.dt >= '%(ld_1monthago_begin)s'
             and t5.dt < '%(ld_1monthago_end)s'
           where t.dt = '%(ld_1monthago_begin)s'
             and t1.fuid is null
         """) % query
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 组合
        hql = ("""insert overwrite table bud_dm.bud_user_act_monthly_detail
            partition(dt='%(ld_monthbegin)s')
            select '%(ld_monthbegin)s' fdate
                   ,fgamefsk
                   ,fplatformfsk
                   ,fhallfsk
                   ,fgame_id fsubgamefsk
                   ,fterminaltypefsk
                   ,fversionfsk
                   ,sum(fact_unum) fact_unum
                   ,sum(floss_new_unum) floss_new_unum
                   ,sum(floss_old_unum) floss_old_unum
                   ,sum(fret_unum) fret_unum
                   ,sum(fref_unum) fref_unum
                   ,sum(fnew_unum) fnew_unum
              from (
                    select fgamefsk
                           ,fplatformfsk
                           ,fhallfsk
                           ,fgame_id
                           ,fterminaltypefsk
                           ,fversionfsk
                           ,count(distinct fuid) fact_unum
                           ,0 floss_new_unum
                           ,0 floss_old_unum
                           ,count(distinct case when act_mon_2 = 1 then fuid end) fret_unum
                           ,count(distinct case when act_new = 0 and act_mon_2 = 0 then fuid end) fref_unum
                           ,count(distinct case when act_new = 1 then fuid end) fnew_unum
                      from work.bud_user_act_monthly_detail_tmp_1_%(statdatenum)s t
                     group by fgamefsk
                              ,fplatformfsk
                              ,fhallfsk
                              ,fgame_id
                              ,fterminaltypefsk
                              ,fversionfsk
                     union all

                    select fgamefsk
                           ,fplatformfsk
                           ,fhallfsk
                           ,fgame_id
                           ,fterminaltypefsk
                           ,fversionfsk
                           ,0 fact_unum
                           ,count(distinct case when act_new = 1 then fuid end) floss_new_unum
                           ,count(distinct case when act_new = 0 then fuid end) floss_old_unum
                           ,0 fret_unum
                           ,0 fref_unum
                           ,0 fnew_unum
                      from work.bud_user_act_monthly_detail_tmp_2_%(statdatenum)s t
                     group by fgamefsk
                              ,fplatformfsk
                              ,fhallfsk
                              ,fgame_id
                              ,fterminaltypefsk
                              ,fversionfsk
                 ) t
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
        hql = """drop table if exists work.bud_user_act_monthly_detail_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_act_monthly_detail_tmp_2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_act_monthly_detail(sys.argv[1:])
a()
