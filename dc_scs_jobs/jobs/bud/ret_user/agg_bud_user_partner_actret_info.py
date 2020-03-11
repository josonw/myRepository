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


class agg_bud_user_partner_actret_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_partner_actret_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               flast_date          date      comment '活跃（新增）日期',
               fact_unum           bigint    comment '当日活跃用户',
               fact_gameret_unum   bigint    comment '活跃子游戏留存',
               fact_hallret_unum   bigint    comment '活跃大厅留存',
               freg_unum           bigint    comment '当日新增用户',
               freg_gameret_unum   bigint    comment '新增子游戏留存',
               freg_hallret_unum   bigint    comment '新增大厅留存'
               )comment '代理商用户活跃留存'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_partner_actret_info';
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
                 'null_str_group_rule': sql_const.NULL_STR_GROUP_RULE,
                 'null_int_group_rule': sql_const.NULL_INT_GROUP_RULE,
                 'statdatenum': """%(statdatenum)s""",
                 'statdate': """%(statdate)s"""}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--活跃
            drop table if exists work.bud_user_partner_actret_info_tmp_1_%(statdatenum)s;
          create table work.bud_user_partner_actret_info_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid reg_uid
                 ,t2.fuid act_uid
                 ,t1.dt flast_date
                 ,case when t1.fgame_id = t2.fgame_id then 1 else 0 end is_game
                 ,case when t1.dt = t3.dt then 1 else 0 end is_new
            from dim.user_act t1
            left join dim.user_act t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t2.dt = '%(statdate)s'
            join dim.reg_user_share t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where ((t1.dt >= '%(ld_7day_ago)s' and t1.dt <= '%(statdate)s')
              or t1.dt='%(ld_14day_ago)s'
              or t1.dt='%(ld_30day_ago)s'
              or t1.dt='%(ld_60day_ago)s'
              or t1.dt='%(ld_90day_ago)s');
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
                 ,flast_date
                 ,count(distinct reg_uid) fact_unum
                 ,count(distinct case when is_game = 1 then act_uid end) fact_gameret_unum
                 ,count(distinct act_uid) fact_hallret_unum
                 ,count(distinct case when is_new = 1 then reg_uid end) freg_unum
                 ,count(distinct case when is_new = 1 and is_game = 1 then act_uid end) freg_gameret_unum
                 ,count(distinct case when is_new = 1 then act_uid end) freg_hallret_unum
            from work.bud_user_partner_actret_info_tmp_1_%(statdatenum)s t
           where fgame_id <> -13658
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,flast_date
         """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_user_partner_actret_info
                      partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总2
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,%(null_int_group_rule)d fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,flast_date
                 ,count(distinct reg_uid) fact_unum
                 ,count(distinct act_uid) fact_gameret_unum
                 ,count(distinct act_uid) fact_hallret_unum
                 ,count(distinct case when is_new = 1 then reg_uid end) freg_unum
                 ,count(distinct case when is_new = 1 then act_uid end) freg_gameret_unum
                 ,count(distinct case when is_new = 1 then act_uid end) freg_hallret_unum
            from work.bud_user_partner_actret_info_tmp_1_%(statdatenum)s t
           where fgame_id = -13658
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,flast_date
         """

        # 组合
        hql = (
            """insert into table bud_dm.bud_user_partner_actret_info
                      partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_2)s  """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_partner_actret_info_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_partner_actret_info(sys.argv[1:])
a()
