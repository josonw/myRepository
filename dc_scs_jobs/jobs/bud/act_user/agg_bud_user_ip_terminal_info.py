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


class agg_bud_user_ip_terminal_info(BaseStatModel):

    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_ip_terminal_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               freg_terminal_unum  bigint         comment '新增用户终端数',
               freg_ip_cnt         bigint         comment '新增用户ip数',
               fact_terminal_unum  bigint         comment '活跃用户终端数',
               fact_ip_cnt         bigint         comment '活跃用户ip数'
               )comment '用户ip终端信息表'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_ip_terminal_info';
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
                               (fgamefsk, fgame_id) )"""

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk),
                               (fgamefsk, fplatformfsk, fhallfsk),
                               (fgamefsk, fplatformfsk) )"""

        extend_group_3 = """
                grouping sets ((fgamefsk, fplatformfsk, fterminaltypefsk, fversionfsk),
                               (fgamefsk, fplatformfsk) )"""

        query = {'extend_group_1': extend_group_1,
                 'extend_group_2': extend_group_2,
                 'extend_group_3': extend_group_3,
                 'null_str_group_rule': sql_const.NULL_STR_GROUP_RULE,
                 'null_int_group_rule': sql_const.NULL_INT_GROUP_RULE,
                 'statdatenum': """%(statdatenum)s""",
                 'statdate': """%(statdate)s"""}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_ip_terminal_info_tmp_a_%(statdatenum)s;
          create table work.bud_user_ip_terminal_info_tmp_a_%(statdatenum)s as
          select distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,fuid
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,fip fip
                 ,fm_imei fm_imei
            from (select fbpid, fuid, fgame_id, fip, fm_imei
                    from stage.user_enter_stg
                   where dt= '%(statdate)s'
                     and fis_first = 1
                   union all
                  select fbpid, fuid, %(null_int_report)d fgame_id, fip, fm_imei
                    from dim.reg_user_main_additional
                   where dt= '%(statdate)s'
                 ) t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,count(distinct fm_imei) freg_terminal_unum    --新增用户终端数
                 ,count(distinct fip)     freg_ip_cnt   --新增用户ip数
                 ,0 fact_terminal_unum               --活跃用户终端数
                 ,0 fact_ip_cnt                      --活跃用户ip数
            from work.bud_user_ip_terminal_info_tmp_a_%(statdatenum)s t
           where fgame_id <> -13658 and hallmode = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """
            drop table if exists work.bud_user_ip_terminal_info_tmp_%(statdatenum)s;
          create table work.bud_user_ip_terminal_info_tmp_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,%(null_int_group_rule)d fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,count(distinct fm_imei) freg_terminal_unum    --新增用户终端数
                 ,count(distinct fip)     freg_ip_cnt   --新增用户ip数
                 ,0 fact_terminal_unum               --活跃用户终端数
                 ,0 fact_ip_cnt                      --活跃用户ip数
            from work.bud_user_ip_terminal_info_tmp_a_%(statdatenum)s t
           where fgame_id = -13658 and hallmode = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert into table work.bud_user_ip_terminal_info_tmp_%(statdatenum)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,%(null_int_group_rule)d fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,count(distinct fm_imei) freg_terminal_unum    --新增用户终端数
                 ,count(distinct fip)     freg_ip_cnt   --新增用户ip数
                 ,0 fact_terminal_unum               --活跃用户终端数
                 ,0 fact_ip_cnt                      --活跃用户ip数
            from work.bud_user_ip_terminal_info_tmp_a_%(statdatenum)s t
           where hallmode = 0
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert into table work.bud_user_ip_terminal_info_tmp_%(statdatenum)s """ +
            base_hql + """%(extend_group_3)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_ip_terminal_info_tmp_b_%(statdatenum)s;
          create table work.bud_user_ip_terminal_info_tmp_b_%(statdatenum)s as
          select distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fuid
                 ,t1.fgame_id
                 ,t2.fip fip
                 ,t2.fm_imei fm_imei
            from dim.user_act t1
            left join (select fbpid, fuid, fip, fm_imei
                         from dim.user_login_additional
                        where dt= '%(statdate)s'
               ) t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,0 freg_terminal_unum    --新增用户终端数
                 ,0 freg_ip_cnt   --新增用户ip数
                 ,count(distinct fm_imei) fact_terminal_unum               --活跃用户终端数
                 ,count(distinct fip)     fact_ip_cnt                      --活跃用户ip数
            from work.bud_user_ip_terminal_info_tmp_b_%(statdatenum)s t
           where hallmode = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert into table work.bud_user_ip_terminal_info_tmp_%(statdatenum)s """ +
            base_hql + """%(extend_group_1)s """ + """ union all""" +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,0 freg_terminal_unum    --新增用户终端数
                 ,0 freg_ip_cnt   --新增用户ip数
                 ,count(distinct fm_imei) fact_terminal_unum               --活跃用户终端数
                 ,count(distinct fip)     fact_ip_cnt                      --活跃用户ip数
            from work.bud_user_ip_terminal_info_tmp_b_%(statdatenum)s t
           where hallmode = 0
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        # 组合
        hql = (
            """insert into table work.bud_user_ip_terminal_info_tmp_%(statdatenum)s """ +
            base_hql + """%(extend_group_3)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """
          insert overwrite table bud_dm.bud_user_ip_terminal_info partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,t1.fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,sum(freg_terminal_unum) freg_terminal_unum    --新增用户终端数
                 ,sum(freg_ip_cnt) freg_ip_cnt                  --新增用户ip数
                 ,sum(fact_terminal_unum) fact_terminal_unum    --活跃用户终端数
                 ,sum(fact_ip_cnt) fact_ip_cnt                  --活跃用户ip数
            from work.bud_user_ip_terminal_info_tmp_%(statdatenum)s t1
           group by t1.fgamefsk,t1.fplatformfsk,t1.fhallfsk,t1.fgame_id,t1.fterminaltypefsk,t1.fversionfsk """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_ip_terminal_info_tmp_%(statdatenum)s;
                 drop table if exists work.bud_user_ip_terminal_info_tmp_a_%(statdatenum)s;
                 drop table if exists work.bud_user_ip_terminal_info_tmp_b_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_ip_terminal_info(sys.argv[1:])
a()
