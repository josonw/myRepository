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


class agg_bud_partner_module_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_partner_module_info (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fnew_partner_unum     bigint         comment  '新增代理商',
               fact_partner_unum     bigint         comment  '活跃代理商',
               fnew_module_unum      bigint         comment  '新增且登录代理商',
               fact_module_unum      bigint         comment  '活跃且登录代理商',
               fnew_eff_unum         bigint         comment  '新增且发展代理商',
               fact_eff_unum         bigint         comment  '活跃且发展代理商',
               fnew_module_eff_unum  bigint         comment  '新增有效代理商',
               fact_module_eff_unum  bigint         comment  '活跃有效代理商'
               )comment '代理商模块用户表'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_partner_module_info';
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
        hql = """--取新增代理商
            drop table if exists work.bud_partner_module_info_tmp_1_%(statdatenum)s;
          create table work.bud_partner_module_info_tmp_1_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,1 is_new
                 ,0 is_act
                 ,case when t3.fpartner_info is not null then 1 else 0 end is_module
                 ,case when t4.fpartner_info is not null then 1 else 0 end is_eff
            from stage.partner_join_stg t1  --发展代理商
            left join stage.partner_event_stg t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fpartner_info
             and t3.dt = '%(statdate)s'
            left join stage.partner_bind_stg t4
              on t1.fuid = t4.fpartner_info
             and t4.dt = '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,0 is_new
                 ,1 is_act
                 ,case when t3.fpartner_info is not null then 1 else 0 end is_module
                 ,case when t4.fpartner_info is not null then 1 else 0 end is_eff
            from (select distinct fbpid,fuid
                    from dim.user_login_additional
                   where dt = '%(statdate)s'
                     and fpartner_info is not null
                     and fuid = fpartner_info
                 ) t1  --代理商活跃
            left join stage.partner_event_stg t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fpartner_info
             and t3.dt = '%(statdate)s'
            left join stage.partner_bind_stg t4
              on t1.fuid = t4.fpartner_info
             and t4.dt = '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
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
                 ,count(distinct case when is_new = 1 then fuid end ) fnew_partner_unum   --新增代理商
                 ,count(distinct case when is_act = 1 then fuid end ) fact_partner_unum   --活跃代理商
                 ,count(distinct case when is_new = 1 and is_module = 1 then fuid end ) fnew_module_unum    --新增且登录代理商
                 ,count(distinct case when is_act = 1 and is_module = 1 then fuid end ) fact_module_unum    --活跃且登录代理商
                 ,count(distinct case when is_new = 1 and is_eff = 1 then fuid end ) fnew_eff_unum       --新增且发展代理商
                 ,count(distinct case when is_act = 1 and is_eff = 1 then fuid end ) fact_eff_unum       --活跃且发展代理商
                 ,count(distinct case when is_new = 1 and is_module = 1 and is_eff = 1 then fuid end ) fnew_module_eff_unum  --新增且登录有效代理商
                 ,count(distinct case when is_act = 1 and is_module = 1 and is_eff = 1 then fuid end ) fact_module_eff_unum  --活跃且登录有效代理商
            from work.bud_partner_module_info_tmp_1_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_partner_module_info  partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_partner_module_info_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_partner_module_info(sys.argv[1:])
a()
