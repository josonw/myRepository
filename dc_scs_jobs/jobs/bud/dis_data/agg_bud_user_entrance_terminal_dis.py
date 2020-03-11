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


class agg_bud_user_entrance_terminal_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_entrance_terminal_dis (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fentrance_info        varchar(20)      comment '账号类型',
               reg_unum              bigint           comment '新增用户设备数',
               game_unum             bigint           comment '子游戏登录设备数',
               act_unum              bigint           comment '活跃用户设备数',
               play_unum             bigint           comment '玩牌用户设备数',
               rupt_unum             bigint           comment '破产用户设备数',
               pay_unum              bigint           comment '付费用户设备数',
               first_pay_unum        bigint           comment '首付用户设备数',
               reg_pay_unum          bigint           comment '注册付费用户设备数'
               )comment '用户账号类型分布_设备数'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_entrance_terminal_dis';
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
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fentrance_id),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fentrance_id),
                               (fgamefsk, fgame_id, fentrance_id) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fentrance_id),
                               (fgamefsk, fplatformfsk, fhallfsk, fentrance_id),
                               (fgamefsk, fplatformfsk, fentrance_id) ) """

        extend_group_3 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fentrance_id),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fentrance_id),
                               (fgamefsk, fgame_id, fentrance_id) )  """

        extend_group_4 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fentrance_id),
                               (fgamefsk, fplatformfsk, fhallfsk, fentrance_id),
                               (fgamefsk, fplatformfsk, fentrance_id) ) """

        query = {'extend_group_1': extend_group_1,
                 'extend_group_2': extend_group_2,
                 'extend_group_3': extend_group_3,
                 'extend_group_4': extend_group_4,
                 'null_str_group_rule': sql_const.NULL_STR_GROUP_RULE,
                 'null_int_group_rule': sql_const.NULL_INT_GROUP_RULE,
                 'statdatenum': """%(statdatenum)s""",
                 'statdate': """%(statdate)s"""}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据--设备号
        hql = """--
            drop table if exists work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s;
          create table work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s as
          select t1.fbpid
                 ,fuid
                 ,max(fm_imei) fm_imei
            from (select fbpid, fuid, coalesce(fm_imei, '0') fm_imei
                    from dim.user_login_additional
                   where dt= '%(statdate)s'
                   union all
                  select fbpid, fuid, coalesce(fm_imei, '0') fm_imei
                    from dim.reg_user_main_additional
                   where dt= '%(statdate)s'
                 ) t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
            group by t1.fbpid, fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据--版本
        hql = """--
            drop table if exists work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s;
          create table work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s as
          select t1.fbpid
                 ,fuid
                 ,max(fentrance_id) fentrance_id
            from (select fbpid, fuid, coalesce(fentrance_id, '0') fentrance_id
                    from dim.user_login_additional
                   where dt= '%(statdate)s'
                   union all
                  select fbpid, fuid, coalesce(fentrance_id, '0') fentrance_id
                    from dim.reg_user_main_additional
                   where dt= '%(statdate)s'
                 ) t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
            group by t1.fbpid, fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_entrance_terminal_dis_tmp_2_%(statdatenum)s;
          create table work.bud_user_entrance_terminal_dis_tmp_2_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t2.fentrance_id, '0') fentrance_id
                 ,coalesce(t4.fm_imei, '0') fm_imei
                 ,t1.fuid
                 ,1 type
                 ,t3.ftotal_usd_amt type_num
            from dim.reg_user_main_additional t1
            left join dim.user_pay_day t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt = '%(statdate)s'
            left join work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s t4
              on t1.fbpid = t4.fbpid
             and t1.fuid = t4.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t2.fentrance_id, '0') fentrance_id
                 ,coalesce(t3.fm_imei, '0') fm_imei
                 ,t1.fuid
                 ,2 type
                 ,0 type_num
            from dim.user_login_additional t1
            left join work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
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
                 ,coalesce(t2.fentrance_id, '0') fentrance_id
                 ,coalesce(t3.fm_imei, '0') fm_imei
                 ,t1.fuid
                 ,3 type
                 ,frupt_cnt type_num
            from dim.user_bankrupt_relieve t1
            left join work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t2.fentrance_id, '0') fentrance_id
                 ,coalesce(t3.fm_imei, '0') fm_imei
                 ,t1.fuid
                 ,4 type
                 ,fparty_num type_num
            from dim.user_act t1
            left join work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t2.fentrance_id, '0') fentrance_id
                 ,coalesce(t3.fm_imei, '0') fm_imei
                 ,t1.fuid
                 ,5 type
                 ,ftotal_usd_amt type_num
            from dim.user_pay_day t1
            left join work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
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
                 ,coalesce(t2.fentrance_id, '0') fentrance_id
                 ,coalesce(t3.fm_imei, '0') fm_imei
                 ,t1.fuid
                 ,6 type
                 ,cast (0 as decimal(20,2)) type_num
            from dim.user_pay t1
            left join work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
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
                 ,fentrance_id fentrance_info         --版本类型
                 ,count(distinct case when type = 1 then fm_imei end) reg_unum               --新增用户数
                 ,count(distinct case when type = 2 then fm_imei end) game_unum              --登录用户数
                 ,count(distinct case when type = 4 then fm_imei end)  act_unum              --活跃用户数
                 ,count(distinct case when type = 4 and type_num >0 then fm_imei end) play_unum              --玩牌用户数
                 ,count(distinct case when type = 3 then fm_imei end) rupt_unum              --破产用户数
                 ,count(distinct case when type = 5 then fm_imei end) pay_unum               --付费用户数
                 ,count(distinct case when type = 6 then fm_imei end) first_pay_unum         --首付用户数
                 ,count(distinct case when type = 1 and type_num > 0 then fm_imei end) reg_pay_unum           --注册付费用户数
            from work.bud_user_entrance_terminal_dis_tmp_2_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fentrance_id
         """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_user_entrance_terminal_dis
                      partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_4)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据--设备号
        hql = """--
            drop table if exists work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s;
          create table work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s as
          select t1.fbpid
                 ,fuid
                 ,max(fm_imei) fm_imei
            from (select fbpid, fuid, coalesce(fm_imei, '0') fm_imei
                    from stage.user_enter_stg
                   where dt= '%(statdate)s'
                 ) t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
            group by t1.fbpid, fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据--版本
        hql = """--
            drop table if exists work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s;
          create table work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s as
          select t1.fbpid
                 ,fuid
                 ,max(fentrance_id) fentrance_id
            from (select fbpid, fuid, coalesce(fentrance_id, '0') fentrance_id
                    from stage.user_enter_stg
                   where dt= '%(statdate)s'
                 ) t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
            group by t1.fbpid, fuid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_entrance_terminal_dis_tmp_3_%(statdatenum)s;
          create table work.bud_user_entrance_terminal_dis_tmp_3_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t2.fentrance_id, '0') fentrance_id
                 ,coalesce(t4.fm_imei, '0') fm_imei
                 ,t1.fuid
                 ,1 type
                 ,t3.ftotal_usd_amt type_num
            from dim.reg_user_sub t1
            left join dim.user_pay_day t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
             and t3.dt = '%(statdate)s'
            left join work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s t4
              on t1.fbpid = t4.fbpid
             and t1.fuid = t4.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and t1.fis_first = 1
           union all
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t2.fentrance_id, '0') fentrance_id
                 ,coalesce(t3.fm_imei, '0') fm_imei
                 ,t1.fuid
                 ,2 type
                 ,0 type_num
            from dim.reg_user_sub t1
            left join work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t2.fentrance_id, '0') fentrance_id
                 ,coalesce(t3.fm_imei, '0') fm_imei
                 ,t1.fuid
                 ,3 type
                 ,frupt_cnt type_num
            from dim.user_bankrupt_relieve t1
            left join work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t2.fentrance_id, '0') fentrance_id
                 ,coalesce(t3.fm_imei, '0') fm_imei
                 ,t1.fuid
                 ,4 type
                 ,fparty_num type_num
            from dim.user_act t1
            left join work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t2.fentrance_id, '0') fentrance_id
                 ,coalesce(t3.fm_imei, '0') fm_imei
                 ,t1.fuid
                 ,5 type
                 ,ftotal_usd_amt type_num
            from dim.user_pay_day t1
            left join work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
           union all
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,coalesce(t2.fentrance_id, '0') fentrance_id
                 ,coalesce(t3.fm_imei, '0') fm_imei
                 ,t1.fuid
                 ,6 type
                 ,cast (0 as decimal(20,2)) type_num
            from dim.user_pay t1
            left join work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s t3
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 1
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fentrance_id fentrance_info         --版本类型
                 ,count(distinct case when type = 1 then fm_imei end) reg_unum               --新增用户数
                 ,count(distinct case when type = 2 then fm_imei end) game_unum              --登录用户数
                 ,count(distinct case when type = 4 then fm_imei end)  act_unum              --活跃用户数
                 ,count(distinct case when type = 4 and type_num >0 then fm_imei end) play_unum              --玩牌用户数
                 ,count(distinct case when type = 3 then fm_imei end) rupt_unum              --破产用户数
                 ,count(distinct case when type = 5 then fm_imei end) pay_unum               --付费用户数
                 ,count(distinct case when type = 6 then fm_imei end) first_pay_unum         --首付用户数
                 ,count(distinct case when type = 1 and type_num > 0 then fm_imei end) reg_pay_unum           --注册付费用户数
            from work.bud_user_entrance_terminal_dis_tmp_3_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fentrance_id
         """

        # 组合
        hql = (
            """insert into table bud_dm.bud_user_entrance_terminal_dis
                      partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_3)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_entrance_terminal_dis_tmp_0_%(statdatenum)s;
                 drop table if exists work.bud_user_entrance_terminal_dis_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_entrance_terminal_dis_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_user_entrance_terminal_dis_tmp_3_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_entrance_terminal_dis(sys.argv[1:])
a()
