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


class agg_bud_user_module_actret_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_moduleact_actret_info (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               flast_date            date      comment '当日日期',
               fact_unum             bigint    comment '当日活跃用户（登陆游戏）',
               fact_ret_unum         bigint    comment '今日登录用户（登录代理商模块）',
               fmodule_unum          bigint    comment '当日登录用户（登录代理商模块）',
               fmodule_ret_unum      bigint    comment '今日登录用户（登录代理商模块）',
               feff_unum             bigint    comment '当日登录用户（登录代理商模块）',
               feff_ret_unum         bigint    comment '今日有效用户（登录且发展用户）',
               fmodule_eff_unum      bigint    comment '当日有效用户（登录且发展用户）',
               fmodule_eff_ret_unum  bigint    comment '今日有效用户（登录且发展用户）'
               )comment '活跃代理商留存'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_moduleact_actret_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        create table if not exists bud_dm.bud_user_modulenew_actret_info (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               flast_date            date      comment '当日日期',
               fnew_unum             bigint    comment '当日新增用户（登陆游戏）',
               fnew_ret_unum         bigint    comment '今日登录用户（登录代理商模块）',
               fmodule_unum          bigint    comment '当日登录用户（登录代理商模块）',
               fmodule_ret_unum      bigint    comment '今日登录用户（登录代理商模块）',
               feff_unum             bigint    comment '当日登录用户（登录代理商模块）',
               feff_ret_unum         bigint    comment '今日有效用户（登录且发展用户）',
               fmodule_eff_unum      bigint    comment '当日有效用户（登录且发展用户）',
               fmodule_eff_ret_unum  bigint    comment '今日有效用户（登录且发展用户）'
               )comment '新增代理商留存'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_modulenew_actret_info';
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
                               (fgamefsk, fgame_id, flast_date) )
                        union all"""

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
        hql = """--取新增\活跃代理商登录留存
            drop table if exists work.bud_user_module_actret_info_tmp_1_%(statdatenum)s;
          create table work.bud_user_module_actret_info_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,t1.flast_date
                 ,case when t1.type = 1 then 1 else 0 end is_new
                 ,case when t1.type = 2 then 1 else 0 end is_act
                 ,case when t2.fpartner_info is not null then 1 else 0 end is_module
            from (select distinct t1.fbpid
                         ,t1.fuid
                         ,t1.dt flast_date
                         ,1 type
                    from stage.partner_join_stg t1  --发展代理商
                   where ((t1.dt >= '%(ld_7day_ago)s' and t1.dt <= '%(statdate)s')
                            or t1.dt='%(ld_14day_ago)s'
                            or t1.dt='%(ld_30day_ago)s'
                            or t1.dt='%(ld_60day_ago)s'
                            or t1.dt='%(ld_90day_ago)s')
                    union all
                  select distinct t1.fbpid
                         ,t1.fuid
                         ,t1.dt flast_date
                         ,2 type
                    from dim.user_login_additional t1  --活跃代理商
                   where fuid = fpartner_info
                     and fpartner_info is not null
                     and ((t1.dt >= '%(ld_7day_ago)s' and t1.dt <= '%(statdate)s')
                            or t1.dt='%(ld_14day_ago)s'
                            or t1.dt='%(ld_30day_ago)s'
                            or t1.dt='%(ld_60day_ago)s'
                            or t1.dt='%(ld_90day_ago)s')
                 ) t1
            left join stage.partner_event_stg t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fpartner_info
             and t2.dt = '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总一代理商系统
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,flast_date           --当日日期
                 ,count(distinct case when is_act = 1 then fuid end) fact_unum            --活跃_当日活跃用户（登陆游戏）
                 ,count(distinct case when is_act = 1 and is_module = 1 then fuid end) fact_ret_unum        --活跃_今日登录用户（登录代理商模块）
                 ,0 fmodule_unum         --活跃_当日登录用户（登录代理商模块）
                 ,0 fmodule_ret_unum     --活跃_今日登录用户（登录代理商模块）
                 ,0 feff_unum            --活跃_当日登录用户（登录代理商模块）
                 ,0 feff_ret_unum        --活跃_今日有效用户（登录且发展用户）
                 ,0 fmodule_eff_unum     --活跃_当日有效用户（登录且发展用户）
                 ,0 fmodule_eff_ret_unum --活跃_今日有效用户（登录且发展用户）
                 ,count(distinct case when is_new = 1 then fuid end) fnew_act_unum            --新增_当日新增用户（登陆游戏）
                 ,count(distinct case when is_new = 1 and is_module = 1 then fuid end) fnew_act_ret_unum        --新增_今日登录用户（登录代理商模块）
                 ,0 fnew_module_unum         --新增_当日登录用户（登录代理商模块）
                 ,0 fnew_module_ret_unum     --新增_今日登录用户（登录代理商模块）
                 ,0 fnew_eff_unum            --新增_当日登录用户（登录代理商模块）
                 ,0 fnew_eff_ret_unum        --新增_今日有效用户（登录且发展用户）
                 ,0 fnew_module_eff_unum     --新增_当日有效用户（登录且发展用户）
                 ,0 fnew_module_eff_ret_unum --新增_今日有效用户（登录且发展用户）
            from work.bud_user_module_actret_info_tmp_1_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,flast_date """

        # 组合
        hql = (
            """ drop table if exists work.bud_user_module_actret_info_tmp_%(statdatenum)s;
          create table work.bud_user_module_actret_info_tmp_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取登录代理商模块的用户
            drop table if exists work.bud_user_module_actret_info_tmp_22_%(statdatenum)s;
          create table work.bud_user_module_actret_info_tmp_22_%(statdatenum)s as
                  select distinct t1.fbpid
                         ,t1.fuid fpartner_info
                         ,t1.dt flast_date
                         ,1 type
                    from stage.partner_join_stg t1  --发展代理商
                    join stage.partner_event_stg t2
                      on t1.fbpid = t2.fbpid
                     and t1.fuid = t2.fpartner_info
                     and t2.dt = t1.dt
                   where ((t1.dt >= '%(ld_7day_ago)s' and t1.dt <= '%(statdate)s')
                            or t1.dt='%(ld_14day_ago)s'
                            or t1.dt='%(ld_30day_ago)s'
                            or t1.dt='%(ld_60day_ago)s'
                            or t1.dt='%(ld_90day_ago)s')
                    union all
                  select distinct t1.fbpid
                         ,cast (t1.fpartner_info as bigint)
                         ,t1.dt flast_date
                         ,2 type
                    from stage.partner_event_stg t1  --登录代理商
                   where ((t1.dt >= '%(ld_7day_ago)s' and t1.dt <= '%(statdate)s')
                            or t1.dt='%(ld_14day_ago)s'
                            or t1.dt='%(ld_30day_ago)s'
                            or t1.dt='%(ld_60day_ago)s'
                            or t1.dt='%(ld_90day_ago)s') ;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取登录代理商模块登陆留存
            drop table if exists work.bud_user_module_actret_info_tmp_2_%(statdatenum)s;
          create table work.bud_user_module_actret_info_tmp_2_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fpartner_info fuid
                 ,t1.flast_date
                 ,case when t1.type = 1 then 1 else 0 end is_new
                 ,case when t1.type = 2 then 1 else 0 end is_act
                 ,case when t2.fpartner_info is not null then 1 else 0 end is_module
            from work.bud_user_module_actret_info_tmp_22_%(statdatenum)s t1
            left join stage.partner_event_stg t2
              on t1.fbpid = t2.fbpid
             and t1.fpartner_info = t2.fpartner_info
             and t2.dt = '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总一代理商系统
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,flast_date           --当日日期
                 ,0 fact_unum            --活跃_当日活跃用户（登陆游戏）
                 ,0 fact_ret_unum        --活跃_今日登录用户（登录代理商模块）
                 ,count(distinct case when is_act = 1 then fuid end) fmodule_unum         --活跃_当日登录用户（登录代理商模块）
                 ,count(distinct case when is_act = 1 and is_module = 1 then fuid end) fmodule_ret_unum     --活跃_今日登录用户（登录代理商模块）
                 ,0 feff_unum            --活跃_当日登录用户（登录代理商模块）
                 ,0 feff_ret_unum        --活跃_今日有效用户（登录且发展用户）
                 ,0 fmodule_eff_unum     --活跃_当日有效用户（登录且发展用户）
                 ,0 fmodule_eff_ret_unum --活跃_今日有效用户（登录且发展用户）
                 ,0 fnew_act_unum            --新增_当日新增用户（登陆游戏）
                 ,0 fnew_act_ret_unum        --新增_今日登录用户（登录代理商模块）
                 ,count(distinct case when is_new = 1 then fuid end) fnew_module_unum         --新增_当日登录用户（登录代理商模块）
                 ,count(distinct case when is_new = 1 and is_module = 1 then fuid end) fnew_module_ret_unum     --新增_今日登录用户（登录代理商模块）
                 ,0 fnew_eff_unum            --新增_当日登录用户（登录代理商模块）
                 ,0 fnew_eff_ret_unum        --新增_今日有效用户（登录且发展用户）
                 ,0 fnew_module_eff_unum     --新增_当日有效用户（登录且发展用户）
                 ,0 fnew_module_eff_ret_unum --新增_今日有效用户（登录且发展用户）
            from work.bud_user_module_actret_info_tmp_2_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,flast_date """

        # 组合
        hql = (
            """ insert into table work.bud_user_module_actret_info_tmp_%(statdatenum)s """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取登录代理商模块有效留存
            drop table if exists work.bud_user_module_actret_info_tmp_3_%(statdatenum)s;
          create table work.bud_user_module_actret_info_tmp_3_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fpartner_info fuid
                 ,t1.flast_date
                 ,case when t1.type = 1 then 1 else 0 end is_new
                 ,case when t1.type = 2 then 1 else 0 end is_act
                 ,case when t2.fpartner_info is not null and t4.fpartner_info is not null then 1 else 0 end is_module_eff
            from work.bud_user_module_actret_info_tmp_22_%(statdatenum)s t1
            left join stage.partner_event_stg t2
              on t1.fbpid = t2.fbpid
             and t1.fpartner_info = t2.fpartner_info
             and t2.dt = '%(statdate)s'
            left join stage.partner_bind_stg t4
              on t1.fpartner_info = t4.fpartner_info
             and t4.dt = '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总一代理商系统
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,flast_date           --当日日期
                 ,0 fact_unum            --活跃_当日活跃用户（登陆游戏）
                 ,0 fact_ret_unum        --活跃_今日登录用户（登录代理商模块）
                 ,0 fmodule_unum         --活跃_当日登录用户（登录代理商模块）
                 ,0 fmodule_ret_unum     --活跃_今日登录用户（登录代理商模块）
                 ,count(distinct case when is_act = 1 then fuid end) feff_unum            --活跃_当日登录用户（登录代理商模块）
                 ,count(distinct case when is_act = 1 and is_module_eff = 1 then fuid end) feff_ret_unum        --活跃_今日有效用户（登录且发展用户）
                 ,0 fmodule_eff_unum     --活跃_当日有效用户（登录且发展用户）
                 ,0 fmodule_eff_ret_unum --活跃_今日有效用户（登录且发展用户）
                 ,0 fnew_act_unum            --新增_当日新增用户（登陆游戏）
                 ,0 fnew_act_ret_unum        --新增_今日登录用户（登录代理商模块）
                 ,0 fnew_module_unum         --新增_当日登录用户（登录代理商模块）
                 ,0 fnew_module_ret_unum     --新增_今日登录用户（登录代理商模块）
                 ,count(distinct case when is_new = 1 then fuid end) fnew_eff_unum            --新增_当日登录用户（登录代理商模块）
                 ,count(distinct case when is_new = 1 and is_module_eff = 1 then fuid end) fnew_eff_ret_unum        --新增_今日有效用户（登录且发展用户）
                 ,0 fnew_module_eff_unum     --新增_当日有效用户（登录且发展用户）
                 ,0 fnew_module_eff_ret_unum --新增_今日有效用户（登录且发展用户）
            from work.bud_user_module_actret_info_tmp_3_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,flast_date """

        # 组合
        hql = (
            """ insert into table work.bud_user_module_actret_info_tmp_%(statdatenum)s """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取有效代理商
            drop table if exists work.bud_user_module_actret_info_tmp_4_%(statdatenum)s;
          create table work.bud_user_module_actret_info_tmp_4_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fpartner_info fuid
                 ,t1.flast_date
                 ,case when t1.type = 1 then 1 else 0 end is_new
                 ,case when t1.type = 2 then 1 else 0 end is_act
                 ,case when t2.fpartner_info is not null and t4.fpartner_info is not null then 1 else 0 end is_module_eff
            from (select distinct t1.fbpid
                         ,t1.fuid fpartner_info
                         ,t1.dt flast_date
                         ,1 type
                    from stage.partner_join_stg t1  --发展代理商
                    join stage.partner_event_stg t2
                      on t1.fbpid = t2.fbpid
                     and t1.fuid = t2.fpartner_info
                     and t2.dt = t1.dt
                    join stage.partner_bind_stg t4
                      on t1.fuid = t4.fpartner_info
                     and t1.dt = t4.dt
                   where ((t1.dt >= '%(ld_7day_ago)s' and t1.dt <= '%(statdate)s')
                            or t1.dt='%(ld_14day_ago)s'
                            or t1.dt='%(ld_30day_ago)s'
                            or t1.dt='%(ld_60day_ago)s'
                            or t1.dt='%(ld_90day_ago)s')
                    union all
                  select distinct t1.fbpid
                         ,cast (t1.fpartner_info as bigint)
                         ,t1.dt flast_date
                         ,2 type
                    from stage.partner_event_stg t1  --登录代理商
                    join stage.partner_bind_stg t4
                      on t1.fpartner_info = t4.fpartner_info
                     and t1.dt = t1.dt
                   where ((t1.dt >= '%(ld_7day_ago)s' and t1.dt <= '%(statdate)s')
                            or t1.dt='%(ld_14day_ago)s'
                            or t1.dt='%(ld_30day_ago)s'
                            or t1.dt='%(ld_60day_ago)s'
                            or t1.dt='%(ld_90day_ago)s')
                 ) t1
            left join stage.partner_event_stg t2
              on t1.fbpid = t2.fbpid
             and t1.fpartner_info = t2.fpartner_info
             and t2.dt = '%(statdate)s'
            left join stage.partner_bind_stg t4
              on t1.fpartner_info = t4.fpartner_info
             and t4.dt = '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总一代理商系统
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,flast_date           --当日日期
                 ,0 fact_unum            --活跃_当日活跃用户（登陆游戏）
                 ,0 fact_ret_unum        --活跃_今日登录用户（登录代理商模块）
                 ,0 fmodule_unum         --活跃_当日登录用户（登录代理商模块）
                 ,0 fmodule_ret_unum     --活跃_今日登录用户（登录代理商模块）
                 ,0 feff_unum            --活跃_当日登录用户（登录代理商模块）
                 ,0 feff_ret_unum        --活跃_今日有效用户（登录且发展用户）
                 ,count(distinct case when is_act = 1 then fuid end) fmodule_eff_unum     --活跃_当日有效用户（登录且发展用户）
                 ,count(distinct case when is_act = 1 and is_module_eff = 1 then fuid end) fmodule_eff_ret_unum --活跃_今日有效用户（登录且发展用户）
                 ,0 fnew_act_unum            --新增_当日新增用户（登陆游戏）
                 ,0 fnew_act_ret_unum        --新增_今日登录用户（登录代理商模块）
                 ,0 fnew_module_unum         --新增_当日登录用户（登录代理商模块）
                 ,0 fnew_module_ret_unum     --新增_今日登录用户（登录代理商模块）
                 ,0 fnew_eff_unum            --新增_当日登录用户（登录代理商模块）
                 ,0 fnew_eff_ret_unum        --新增_今日有效用户（登录且发展用户）
                 ,count(distinct case when is_new = 1 then fuid end) fnew_module_eff_unum     --新增_当日有效用户（登录且发展用户）
                 ,count(distinct case when is_new = 1 and is_module_eff = 1 then fuid end) fnew_module_eff_ret_unum --新增_今日有效用户（登录且发展用户）
            from work.bud_user_module_actret_info_tmp_4_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,flast_date """

        # 组合
        hql = (
            """ insert into table work.bud_user_module_actret_info_tmp_%(statdatenum)s """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg
        hql = """
        insert overwrite table bud_dm.bud_user_modulenew_actret_info partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fgame_id
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,flast_date
                 ,sum(fnew_act_unum) fnew_unum
                 ,sum(fnew_act_ret_unum) fnew_ret_unum
                 ,sum(fnew_module_unum) fmodule_unum
                 ,sum(fnew_module_ret_unum) fmodule_ret_unum
                 ,sum(fnew_eff_unum) feff_unum
                 ,sum(fnew_eff_ret_unum) feff_ret_unum
                 ,sum(fnew_module_eff_unum) fmodule_eff_unum
                 ,sum(fnew_module_eff_ret_unum) fmodule_eff_ret_unum
            from work.bud_user_module_actret_info_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,flast_date
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总act
        hql = """
        insert overwrite table bud_dm.bud_user_moduleact_actret_info partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fgame_id
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,flast_date
                 ,sum(fact_unum) fact_unum
                 ,sum(fact_ret_unum) fact_ret_unum
                 ,sum(fmodule_unum) fmodule_unum
                 ,sum(fmodule_ret_unum) fmodule_ret_unum
                 ,sum(feff_unum) feff_unum
                 ,sum(feff_ret_unum) feff_ret_unum
                 ,sum(fmodule_eff_unum) fmodule_eff_unum
                 ,sum(fmodule_eff_ret_unum) fmodule_eff_ret_unum
            from work.bud_user_module_actret_info_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,flast_date
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_module_actret_info_tmp_%(statdatenum)s;
                 drop table if exists work.bud_user_module_actret_info_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_module_actret_info_tmp_22_%(statdatenum)s;
                 drop table if exists work.bud_user_module_actret_info_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_user_module_actret_info_tmp_3_%(statdatenum)s;
                 drop table if exists work.bud_user_module_actret_info_tmp_4_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_module_actret_info(sys.argv[1:])
a()
