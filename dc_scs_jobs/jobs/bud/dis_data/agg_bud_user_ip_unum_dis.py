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


class agg_bud_user_ip_unum_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_ip_unum_dis (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fuser_type            varchar(10)      comment '用户类型:reg_user,act_user',
               num_1                 bigint           comment '1个用户的ip数',
               num_2                 bigint           comment '2个用户的ip数',
               num_3                 bigint           comment '3个用户的ip数',
               num_4                 bigint           comment '4个用户的ip数',
               num_5                 bigint           comment '5个用户的ip数',
               num_6                 bigint           comment '6个用户的ip数',
               num_7                 bigint           comment '7个用户的ip数',
               num_8                 bigint           comment '8个用户的ip数',
               num_9                 bigint           comment '9个用户的ip数',
               num_10                bigint           comment '10个用户的ip数',
               num_m10               bigint           comment '11-49',
               num_m50               bigint           comment '50-99',
               num_m100              bigint           comment '100-499',
               num_m500              bigint           comment '超过500个用户的ip数'
               )comment '单ip账号数据量'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_ip_unum_dis';

        create table if not exists bud_dm.bud_user_terminal_unum_dis (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fuser_type            varchar(10)      comment '用户类型:reg_user,act_user',
               num_1                 bigint           comment '1个用户的终端数',
               num_2                 bigint           comment '2个用户的终端数',
               num_3                 bigint           comment '3个用户的终端数',
               num_4                 bigint           comment '4个用户的终端数',
               num_5                 bigint           comment '5个用户的终端数',
               num_6                 bigint           comment '6个用户的终端数',
               num_7                 bigint           comment '7个用户的终端数',
               num_8                 bigint           comment '8个用户的终端数',
               num_9                 bigint           comment '9个用户的终端数',
               num_10                bigint           comment '10个用户的终端数',
               num_m10               bigint           comment '超过10个用户的终端数'
               )comment '单设备账号数据量'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_terminal_unum_dis';

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
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fip),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fip),
                               (fgamefsk, fgame_id, fip) ) """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fip),
                               (fgamefsk, fplatformfsk, fhallfsk, fip),
                               (fgamefsk, fplatformfsk, fip) ) """

        extend_group_3 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fm_imei),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fm_imei),
                               (fgamefsk, fgame_id, fm_imei) ) """

        extend_group_4 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fm_imei),
                               (fgamefsk, fplatformfsk, fhallfsk, fm_imei),
                               (fgamefsk, fplatformfsk, fm_imei) ) """

        extend_group_5 = """
                grouping sets ((fgamefsk, fplatformfsk, fterminaltypefsk, fversionfsk, fip),
                               (fgamefsk, fplatformfsk, fip) )"""

        extend_group_6 = """
                grouping sets ((fgamefsk, fplatformfsk, fterminaltypefsk, fversionfsk, fm_imei),
                               (fgamefsk, fplatformfsk, fm_imei) )"""

        query = {'extend_group_1': extend_group_1,
                 'extend_group_2': extend_group_2,
                 'extend_group_3': extend_group_3,
                 'extend_group_4': extend_group_4,
                 'extend_group_5': extend_group_5,
                 'extend_group_6': extend_group_6,
                 'null_str_group_rule': sql_const.NULL_STR_GROUP_RULE,
                 'null_int_group_rule': sql_const.NULL_INT_GROUP_RULE,
                 'statdatenum': """%(statdatenum)s""",
                 'statdate': """%(statdate)s"""}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_ip_unum_dis_tmp_1_%(statdatenum)s;
          create table work.bud_user_ip_unum_dis_tmp_1_%(statdatenum)s as
          select distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,fuid
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,fip fip
                 ,fm_imei
            from (select fbpid, fuid, fgame_id, coalesce(fip, '0') fip, coalesce(fm_imei, '0') fm_imei
                    from stage.user_enter_stg
                   where dt= '%(statdate)s'
                     and fis_first = 1
                   union all
                  select fbpid, fuid, %(null_int_report)d fgame_id, coalesce(fip, '0') fip, coalesce(fm_imei, '0') fm_imei
                    from dim.reg_user_main_additional
                   where dt= '%(statdate)s'
                 ) t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取用户ip数、设备数组合去重数据
        base_hql = """--
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fip
                 ,count(distinct fuid) ip_num
            from work.bud_user_ip_unum_dis_tmp_1_%(statdatenum)s t
           where fgame_id <> -13658 and hallmode = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fip
        """

        # 组合
        hql = (
            """
            drop table if exists work.bud_user_ip_unum_dis_tmp_2_%(statdatenum)s;
        create table work.bud_user_ip_unum_dis_tmp_2_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取用户ip数、设备数组合去重数据
        base_hql = """--
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,%(null_int_group_rule)d fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fip
                 ,count(distinct fuid) ip_num
            from work.bud_user_ip_unum_dis_tmp_1_%(statdatenum)s t
           where fgame_id = -13658 and hallmode = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fip
        """

        # 组合
        hql = (
            """
            insert into table work.bud_user_ip_unum_dis_tmp_2_%(statdatenum)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取用户ip数、设备数组合去重数据
        base_hql = """--
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,%(null_int_group_rule)d fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fip
                 ,count(distinct fuid) ip_num
            from work.bud_user_ip_unum_dis_tmp_1_%(statdatenum)s t
           where hallmode = 0
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fip
        """

        # 组合
        hql = (
            """
            insert into table work.bud_user_ip_unum_dis_tmp_2_%(statdatenum)s """ +
            base_hql + """%(extend_group_5)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取新增数据
        hql = """--
          insert overwrite table bud_dm.bud_user_ip_unum_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,t1.fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,'reg_user' fuser_type
                 ,count(distinct case when ip_num = 1 then t1.fip end) num_1
                 ,count(distinct case when ip_num = 2 then t1.fip end) num_2
                 ,count(distinct case when ip_num = 3 then t1.fip end) num_3
                 ,count(distinct case when ip_num = 4 then t1.fip end) num_4
                 ,count(distinct case when ip_num = 5 then t1.fip end) num_5
                 ,count(distinct case when ip_num = 6 then t1.fip end) num_6
                 ,count(distinct case when ip_num = 7 then t1.fip end) num_7
                 ,count(distinct case when ip_num = 8 then t1.fip end) num_8
                 ,count(distinct case when ip_num = 9 then t1.fip end) num_9
                 ,count(distinct case when ip_num = 10 then t1.fip end) num_10
                 ,count(distinct case when ip_num >= 11 and ip_num >= 49 then t1.fip end) num_m10
                 ,count(distinct case when ip_num >= 50 and ip_num >= 99 then t1.fip end) num_m50
                 ,count(distinct case when ip_num >= 100 and ip_num >= 499 then t1.fip end) num_m100
                 ,count(distinct case when ip_num >= 500 then t1.fip end) num_m500
            from work.bud_user_ip_unum_dis_tmp_2_%(statdatenum)s t1
           group by t1.fgamefsk,t1.fplatformfsk,t1.fhallfsk,t1.fgame_id,t1.fterminaltypefsk,t1.fversionfsk
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取用户ip数、设备数组合去重数据
        base_hql = """--
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fm_imei
                 ,count(distinct fuid) imei_num
            from work.bud_user_ip_unum_dis_tmp_1_%(statdatenum)s t
           where fgame_id <> -13658 and hallmode = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fm_imei
        """

        # 组合
        hql = (
            """
            drop table if exists work.bud_user_ip_unum_dis_tmp_3_%(statdatenum)s;
        create table work.bud_user_ip_unum_dis_tmp_3_%(statdatenum)s as """ +
            base_hql + """%(extend_group_3)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取用户ip数、设备数组合去重数据
        base_hql = """--
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,%(null_int_group_rule)d fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fm_imei
                 ,count(distinct fuid) imei_num
            from work.bud_user_ip_unum_dis_tmp_1_%(statdatenum)s t
           where fgame_id = -13658 and hallmode = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fm_imei
        """

        # 组合
        hql = (
            """
            insert into table work.bud_user_ip_unum_dis_tmp_3_%(statdatenum)s """ +
            base_hql + """%(extend_group_4)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取用户ip数、设备数组合去重数据
        base_hql = """--
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,%(null_int_group_rule)d fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fm_imei
                 ,count(distinct fuid) imei_num
            from work.bud_user_ip_unum_dis_tmp_1_%(statdatenum)s t
           where hallmode = 0
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fm_imei
        """

        # 组合
        hql = (
            """
            insert into table work.bud_user_ip_unum_dis_tmp_3_%(statdatenum)s """ +
            base_hql + """%(extend_group_6)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取新增数据
        hql = """--
          insert overwrite table bud_dm.bud_user_terminal_unum_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,t1.fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,'reg_user' fuser_type
                 ,count(distinct case when imei_num = 1 then t1.fm_imei end) num_1
                 ,count(distinct case when imei_num = 2 then t1.fm_imei end) num_2
                 ,count(distinct case when imei_num = 3 then t1.fm_imei end) num_3
                 ,count(distinct case when imei_num = 4 then t1.fm_imei end) num_4
                 ,count(distinct case when imei_num = 5 then t1.fm_imei end) num_5
                 ,count(distinct case when imei_num = 6 then t1.fm_imei end) num_6
                 ,count(distinct case when imei_num = 7 then t1.fm_imei end) num_7
                 ,count(distinct case when imei_num = 8 then t1.fm_imei end) num_8
                 ,count(distinct case when imei_num = 9 then t1.fm_imei end) num_9
                 ,count(distinct case when imei_num = 10 then t1.fm_imei end) num_10
                 ,count(distinct case when imei_num > 10 then t1.fm_imei end) num_m10
            from work.bud_user_ip_unum_dis_tmp_3_%(statdatenum)s t1
           group by t1.fgamefsk,t1.fplatformfsk,t1.fhallfsk,t1.fgame_id,t1.fterminaltypefsk,t1.fversionfsk
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取活跃数据
        hql = """
            drop table if exists work.bud_user_ip_unum_dis_tmp_4_%(statdatenum)s;
          create table work.bud_user_ip_unum_dis_tmp_4_%(statdatenum)s as
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
            from (select fbpid, fuid, fgame_id, coalesce(fip, '0') fip, coalesce(fm_imei, '0') fm_imei
                    from stage.user_enter_stg
                   where dt= '%(statdate)s'
                   union all
                  select fbpid, fuid, %(null_int_report)d fgame_id, coalesce(fip, '0') fip, coalesce(fm_imei, '0') fm_imei
                    from dim.user_login_additional
                   where dt= '%(statdate)s'
                 ) t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取用户ip数、设备数组合去重数据
        base_hql = """--
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fip
                 ,count(distinct fuid) ip_num
            from work.bud_user_ip_unum_dis_tmp_4_%(statdatenum)s t
           where hallmode = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fip
        """

        # 组合
        hql = (
            """
            drop table if exists work.bud_user_ip_unum_dis_tmp_5_%(statdatenum)s;
        create table work.bud_user_ip_unum_dis_tmp_5_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """ + """ union all""" +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取用户ip数、设备数组合去重数据
        base_hql = """--
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fip
                 ,count(distinct fuid) ip_num
            from work.bud_user_ip_unum_dis_tmp_4_%(statdatenum)s t
           where hallmode = 0
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fip
        """

        # 组合
        hql = (
            """
            insert into table work.bud_user_ip_unum_dis_tmp_5_%(statdatenum)s """ +
            base_hql + """%(extend_group_5)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取新增数据
        hql = """--
          insert into table bud_dm.bud_user_ip_unum_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,t1.fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,'act_user' fuser_type
                 ,count(distinct case when ip_num = 1 then t1.fip end) num_1
                 ,count(distinct case when ip_num = 2 then t1.fip end) num_2
                 ,count(distinct case when ip_num = 3 then t1.fip end) num_3
                 ,count(distinct case when ip_num = 4 then t1.fip end) num_4
                 ,count(distinct case when ip_num = 5 then t1.fip end) num_5
                 ,count(distinct case when ip_num = 6 then t1.fip end) num_6
                 ,count(distinct case when ip_num = 7 then t1.fip end) num_7
                 ,count(distinct case when ip_num = 8 then t1.fip end) num_8
                 ,count(distinct case when ip_num = 9 then t1.fip end) num_9
                 ,count(distinct case when ip_num = 10 then t1.fip end) num_10
                 ,count(distinct case when ip_num >= 11 and ip_num >= 49 then t1.fip end) num_m10
                 ,count(distinct case when ip_num >= 50 and ip_num >= 99 then t1.fip end) num_m50
                 ,count(distinct case when ip_num >= 100 and ip_num >= 499 then t1.fip end) num_m100
                 ,count(distinct case when ip_num >= 500 then t1.fip end) num_m500
            from work.bud_user_ip_unum_dis_tmp_5_%(statdatenum)s t1
           group by t1.fgamefsk,t1.fplatformfsk,t1.fhallfsk,t1.fgame_id,t1.fterminaltypefsk,t1.fversionfsk
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取用户ip数、设备数组合去重数据
        base_hql = """--
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fm_imei
                 ,count(distinct fuid) imei_num
            from work.bud_user_ip_unum_dis_tmp_4_%(statdatenum)s t
           where hallmode = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fm_imei
        """

        # 组合
        hql = (
            """
            drop table if exists work.bud_user_ip_unum_dis_tmp_6_%(statdatenum)s;
        create table work.bud_user_ip_unum_dis_tmp_6_%(statdatenum)s as """ +
            base_hql + """%(extend_group_3)s """ + """ union all""" +
            base_hql + """%(extend_group_4)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取用户ip数、设备数组合去重数据
        base_hql = """--
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fm_imei
                 ,count(distinct fuid) imei_num
            from work.bud_user_ip_unum_dis_tmp_4_%(statdatenum)s t
           where hallmode = 0
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fm_imei
        """

        # 组合
        hql = (
            """
            insert into table work.bud_user_ip_unum_dis_tmp_6_%(statdatenum)s  """ +
            base_hql + """%(extend_group_6)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取新增数据
        hql = """--
          insert into table bud_dm.bud_user_terminal_unum_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,t1.fgamefsk
                 ,t1.fplatformfsk
                 ,t1.fhallfsk
                 ,t1.fgame_id
                 ,t1.fterminaltypefsk
                 ,t1.fversionfsk
                 ,'act_user' fuser_type
                 ,count(distinct case when imei_num = 1 then t1.fm_imei end) num_1
                 ,count(distinct case when imei_num = 2 then t1.fm_imei end) num_2
                 ,count(distinct case when imei_num = 3 then t1.fm_imei end) num_3
                 ,count(distinct case when imei_num = 4 then t1.fm_imei end) num_4
                 ,count(distinct case when imei_num = 5 then t1.fm_imei end) num_5
                 ,count(distinct case when imei_num = 6 then t1.fm_imei end) num_6
                 ,count(distinct case when imei_num = 7 then t1.fm_imei end) num_7
                 ,count(distinct case when imei_num = 8 then t1.fm_imei end) num_8
                 ,count(distinct case when imei_num = 9 then t1.fm_imei end) num_9
                 ,count(distinct case when imei_num = 10 then t1.fm_imei end) num_10
                 ,count(distinct case when imei_num > 10 then t1.fm_imei end) num_m10
            from work.bud_user_ip_unum_dis_tmp_6_%(statdatenum)s t1
           group by t1.fgamefsk,t1.fplatformfsk,t1.fhallfsk,t1.fgame_id,t1.fterminaltypefsk,t1.fversionfsk
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_ip_unum_dis_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_ip_unum_dis_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_user_ip_unum_dis_tmp_3_%(statdatenum)s;
                 drop table if exists work.bud_user_ip_unum_dis_tmp_4_%(statdatenum)s;
                 drop table if exists work.bud_user_ip_unum_dis_tmp_5_%(statdatenum)s;
                 drop table if exists work.bud_user_ip_unum_dis_tmp_6_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_ip_unum_dis(sys.argv[1:])
a()
