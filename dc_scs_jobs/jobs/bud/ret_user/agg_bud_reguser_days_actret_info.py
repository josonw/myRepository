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


class agg_bud_reguser_days_actret_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_reguser_days_actret_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               flast_date          date      comment '当日日期',
               fnew_unum           bigint    comment '当日新增用户',
               fgame_ret_unum      bigint    comment '子游戏留存',
               fhall_ret_unum      bigint    comment '大厅留存'
               )comment '新增用户时间段留存'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_reguser_days_actret_info';
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
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, flast_date),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, flast_date),
                               (fgamefsk, fgame_id, flast_date) ) """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, flast_date),
                               (fgamefsk, fplatformfsk, fhallfsk, flast_date),
                               (fgamefsk, fplatformfsk, flast_date) ) """

        query = {'extend_group_1': extend_group_1,
                 'extend_group_2': extend_group_2,
                 'null_int_report': sql_const.NULL_INT_REPORT,
                 'null_str_group_rule': sql_const.NULL_STR_GROUP_RULE,
                 'null_int_group_rule': sql_const.NULL_INT_GROUP_RULE,
                 'statdatenum': """%(statdatenum)s""",
                 'statdate': """%(statdate)s"""}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        dates_dict = PublicFunc.date_define(self.stat_date)
        query.update(dates_dict)

        # 取基础数据
        hql = """--取用户活跃时间
            drop table if exists work.bud_reguser_days_actret_info_tmp_1_%(statdatenum)s;
          create table work.bud_reguser_days_actret_info_tmp_1_%(statdatenum)s as
          select distinct t1.fbpid,t1.fuid,t1.fgame_id,t1.dt
            from dim.user_act t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt between '%(ld_30day_ago)s' and '%(ld_1day_ago)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = ("""--子游戏新增
            drop table if exists work.bud_reguser_days_actret_info_tmp_2_%(statdatenum)s;
          create table work.bud_reguser_days_actret_info_tmp_2_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid reg_uid
                 ,t2.fuid act_uid
                 ,t1.dt flast_date
                 ,datediff(t2.dt,t1.dt) dt_diff
                 ,case when t1.fgame_id = t2.fgame_id then 1 else 0 end is_game
            from dim.reg_user_sub t1
            left join work.bud_reguser_days_actret_info_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.fis_first = 1    --首次进入子游戏
             and (t1.dt='%(ld_7dayago)s'
                or t1.dt='%(ld_14dayago)s'
                or t1.dt='%(ld_30dayago)s')
        """) % query
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
                 ,flast_date
                 ,count(distinct reg_uid) freg_unum
                 ,count(distinct case when dt_diff > 0 and is_game = 1 then act_uid end) fgame_ret_unum
                 ,count(distinct case when dt_diff > 0 then act_uid end) fhall_ret_unum
            from work.bud_reguser_days_actret_info_tmp_2_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,flast_date
         """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_reguser_days_actret_info
                      partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = ("""--非子游戏新增
            drop table if exists work.bud_reguser_days_actret_info_tmp_3_%(statdatenum)s;
          create table work.bud_reguser_days_actret_info_tmp_3_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid reg_uid
                 ,t2.fuid act_uid
                 ,t1.dt flast_date
                 ,datediff(t2.dt,t1.dt) dt_diff
            from dim.reg_user_main_additional t1
            left join work.bud_reguser_days_actret_info_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where (t1.dt='%(ld_7dayago)s'
                or t1.dt='%(ld_14dayago)s'
                or t1.dt='%(ld_30dayago)s')
        """) % query
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
                 ,flast_date
                 ,count(distinct reg_uid) freg_unum
                 ,count(distinct case when dt_diff > 0 then act_uid end) fgame_ret_unum
                 ,count(distinct case when dt_diff > 0 then act_uid end) fhall_ret_unum
            from work.bud_reguser_days_actret_info_tmp_3_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,flast_date
         """

        # 组合
        hql = (
            """insert into table bud_dm.bud_reguser_days_actret_info
                      partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_2)s  """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_reguser_days_actret_info_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_reguser_days_actret_info_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_reguser_days_actret_info_tmp_3_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_reguser_days_actret_info(sys.argv[1:])
a()
