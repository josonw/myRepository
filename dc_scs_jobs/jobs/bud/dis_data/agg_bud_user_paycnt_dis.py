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


class agg_bud_user_paycnt_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_paycnt_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuser_type          varchar(10)      comment '用户类型:reg_user,act_user',
               num_0               bigint           comment '0',
               num_1               bigint           comment '1',
               num_2               bigint           comment '2',
               num_3               bigint           comment '[3,5]',
               num_6               bigint           comment '[6,10]',
               num_11              bigint           comment '[11,15]',
               num_16              bigint           comment '>=16'
               )comment '用户付费次数分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_paycnt_dis';


        create table if not exists bud_dm.bud_user_payincome_dis (
            fdate                 date,
            fgamefsk              bigint,
            fplatformfsk          bigint,
            fhallfsk              bigint,
            fsubgamefsk           bigint,
            fterminaltypefsk      bigint,
            fversionfsk           bigint,
            fuser_type            varchar(10)      comment '用户类型:reg_user,act_user',
            num_0                 bigint           comment '0',
            num_1                 bigint           comment '(0,1]',
            num_2                 bigint           comment '(1,2]',
            num_3                 bigint           comment '(2,3]',
            num_4                 bigint           comment '(3,4]',
            num_5                 bigint           comment '(4,5]',
            num_6                 bigint           comment '(5,6]',
            num_7                 bigint           comment '(6,7]',
            num_8                 bigint           comment '(7,8]',
            num_9                 bigint           comment '(8,9]',
            num_10                bigint           comment '(9,10]',
            num_15                bigint           comment '(10,15]',
            num_m15               bigint           comment '>15'
        )comment '用户付费额度分布'
        partitioned by(dt date)
        location '/dw/bud_dm/bud_user_payincome_dis';

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
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fuid),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fuid),
                               (fgamefsk, fgame_id) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fuid),
                               (fgamefsk, fplatformfsk, fhallfsk, fuid),
                               (fgamefsk, fplatformfsk, fuid) ) """

        extend_group_3 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fuid),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fuid),
                               (fgamefsk, fgame_id, fuid) )  """

        extend_group_4 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fuid),
                               (fgamefsk, fplatformfsk, fhallfsk, fuid),
                               (fgamefsk, fplatformfsk, fuid) ) """

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

        # 取基础数据
        hql = """--取注册相关指标
            drop table if exists work.bud_user_paycnt_dis_tmp_1_%(statdatenum)s;
          create table work.bud_user_paycnt_dis_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t2.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,coalesce(fpay_cnt,0) fpay_cnt
                 ,coalesce(ftotal_usd_amt,0) ftotal_usd_amt
            from dim.reg_user_main_additional t1
            left join dim.user_pay_day t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t2.dt='%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt='%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取子游戏新增相关指标
            drop table if exists work.bud_user_paycnt_dis_tmp_2_%(statdatenum)s;
          create table work.bud_user_paycnt_dis_tmp_2_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,coalesce(fpay_cnt,0) fpay_cnt
                 ,coalesce(ftotal_usd_amt,0) ftotal_usd_amt
            from dim.reg_user_sub t1
            left join (select fbpid,fuid,sum(fpay_cnt) fpay_cnt,sum(ftotal_usd_amt) ftotal_usd_amt
                         from dim.user_pay_day t2
                        where t2.dt='%(statdate)s'
                        group by fbpid,fuid
                      ) t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and t1.fis_first = 1  --首次进入子游戏;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg1
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuid
                 ,sum(fpay_cnt) fpay_cnt
                 ,sum(ftotal_usd_amt) ftotal_usd_amt
            from work.bud_user_paycnt_dis_tmp_1_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fuid
         """

        # 组合
        hql = (
            """ drop table if exists work.bud_user_paycnt_dis_tmp_%(statdatenum)s;
          create table work.bud_user_paycnt_dis_tmp_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg1
        hql = """
        insert overwrite table bud_dm.bud_user_paycnt_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'reg_user' fuser_type
                 ,count(distinct case when fpay_cnt = 0 then fuid end) num_0
                 ,count(distinct case when fpay_cnt = 1 then fuid end) num_1
                 ,count(distinct case when fpay_cnt = 2 then fuid end) num_2
                 ,count(distinct case when fpay_cnt >= 3 and fpay_cnt <= 5 then fuid end) num_3
                 ,count(distinct case when fpay_cnt >= 6 and fpay_cnt <= 10 then fuid end) num_6
                 ,count(distinct case when fpay_cnt >= 11 and fpay_cnt <= 15 then fuid end) num_11
                 ,count(distinct case when fpay_cnt >= 16 then fuid end) num_16
            from work.bud_user_paycnt_dis_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg1
        hql = """
        insert overwrite table bud_dm.bud_user_payincome_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'reg_user' fuser_type
                 ,count(distinct case when ftotal_usd_amt = 0 then fuid end) num_0
                 ,count(distinct case when ftotal_usd_amt > 0 and ftotal_usd_amt <= 1 then fuid end) num_1
                 ,count(distinct case when ftotal_usd_amt > 1 and ftotal_usd_amt <= 2 then fuid end) num_2
                 ,count(distinct case when ftotal_usd_amt > 2 and ftotal_usd_amt <= 3 then fuid end) num_3
                 ,count(distinct case when ftotal_usd_amt > 3 and ftotal_usd_amt <= 4 then fuid end) num_4
                 ,count(distinct case when ftotal_usd_amt > 4 and ftotal_usd_amt <= 5 then fuid end) num_5
                 ,count(distinct case when ftotal_usd_amt > 5 and ftotal_usd_amt <= 6 then fuid end) num_6
                 ,count(distinct case when ftotal_usd_amt > 6 and ftotal_usd_amt <= 7 then fuid end) num_7
                 ,count(distinct case when ftotal_usd_amt > 7 and ftotal_usd_amt <= 8 then fuid end) num_8
                 ,count(distinct case when ftotal_usd_amt > 8 and ftotal_usd_amt <= 9 then fuid end) num_9
                 ,count(distinct case when ftotal_usd_amt > 9 and ftotal_usd_amt <= 10 then fuid end) num_10
                 ,count(distinct case when ftotal_usd_amt > 10 and ftotal_usd_amt <= 15 then fuid end) num_15
                 ,count(distinct case when ftotal_usd_amt > 15 then fuid end) num_m15
            from work.bud_user_paycnt_dis_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg1
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuid
                 ,sum(fpay_cnt) fpay_cnt
                 ,sum(ftotal_usd_amt) ftotal_usd_amt
            from work.bud_user_paycnt_dis_tmp_2_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fuid
         """

        # 组合
        hql = (
            """ drop table if exists work.bud_user_paycnt_dis_tmp_%(statdatenum)s;
          create table work.bud_user_paycnt_dis_tmp_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg1
        hql = """
        insert into table bud_dm.bud_user_paycnt_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'game_user' fuser_type
                 ,count(distinct case when fpay_cnt = 0 then fuid end) num_0
                 ,count(distinct case when fpay_cnt = 1 then fuid end) num_1
                 ,count(distinct case when fpay_cnt = 2 then fuid end) num_2
                 ,count(distinct case when fpay_cnt >= 3 and fpay_cnt <= 5 then fuid end) num_3
                 ,count(distinct case when fpay_cnt >= 6 and fpay_cnt <= 10 then fuid end) num_6
                 ,count(distinct case when fpay_cnt >= 11 and fpay_cnt <= 15 then fuid end) num_11
                 ,count(distinct case when fpay_cnt >= 16 then fuid end) num_16
            from work.bud_user_paycnt_dis_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg1
        hql = """
        insert into table bud_dm.bud_user_payincome_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'game_user' fuser_type
                 ,count(distinct case when ftotal_usd_amt = 0 then fuid end) num_0
                 ,count(distinct case when ftotal_usd_amt > 0 and ftotal_usd_amt <= 1 then fuid end) num_1
                 ,count(distinct case when ftotal_usd_amt > 1 and ftotal_usd_amt <= 2 then fuid end) num_2
                 ,count(distinct case when ftotal_usd_amt > 2 and ftotal_usd_amt <= 3 then fuid end) num_3
                 ,count(distinct case when ftotal_usd_amt > 3 and ftotal_usd_amt <= 4 then fuid end) num_4
                 ,count(distinct case when ftotal_usd_amt > 4 and ftotal_usd_amt <= 5 then fuid end) num_5
                 ,count(distinct case when ftotal_usd_amt > 5 and ftotal_usd_amt <= 6 then fuid end) num_6
                 ,count(distinct case when ftotal_usd_amt > 6 and ftotal_usd_amt <= 7 then fuid end) num_7
                 ,count(distinct case when ftotal_usd_amt > 7 and ftotal_usd_amt <= 8 then fuid end) num_8
                 ,count(distinct case when ftotal_usd_amt > 8 and ftotal_usd_amt <= 9 then fuid end) num_9
                 ,count(distinct case when ftotal_usd_amt > 9 and ftotal_usd_amt <= 10 then fuid end) num_10
                 ,count(distinct case when ftotal_usd_amt > 10 and ftotal_usd_amt <= 15 then fuid end) num_15
                 ,count(distinct case when ftotal_usd_amt > 15 then fuid end) num_m15
            from work.bud_user_paycnt_dis_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取活跃相关指标
            drop table if exists work.bud_user_paycnt_dis_tmp_3_%(statdatenum)s;
          create table work.bud_user_paycnt_dis_tmp_3_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,coalesce(fpay_cnt,0) fpay_cnt
                 ,coalesce(ftotal_usd_amt,0) ftotal_usd_amt
            from dim.user_act t1
            left join dim.user_pay_day t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t1.fgame_id = t2.fgame_id
             and t2.dt='%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg1
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuid
                 ,sum(fpay_cnt) fpay_cnt
                 ,sum(ftotal_usd_amt) ftotal_usd_amt
            from work.bud_user_paycnt_dis_tmp_3_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fuid
         """

        # 组合
        hql = (
            """ drop table if exists work.bud_user_paycnt_dis_tmp_%(statdatenum)s;
          create table work.bud_user_paycnt_dis_tmp_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg1
        hql = """
        insert into table bud_dm.bud_user_paycnt_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'act_user' fuser_type
                 ,count(distinct case when fpay_cnt = 0 then fuid end) num_0
                 ,count(distinct case when fpay_cnt = 1 then fuid end) num_1
                 ,count(distinct case when fpay_cnt = 2 then fuid end) num_2
                 ,count(distinct case when fpay_cnt >= 3 and fpay_cnt <= 5 then fuid end) num_3
                 ,count(distinct case when fpay_cnt >= 6 and fpay_cnt <= 10 then fuid end) num_6
                 ,count(distinct case when fpay_cnt >= 11 and fpay_cnt <= 15 then fuid end) num_11
                 ,count(distinct case when fpay_cnt >= 16 then fuid end) num_16
            from work.bud_user_paycnt_dis_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg1
        hql = """
        insert into table bud_dm.bud_user_payincome_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'act_user' fuser_type
                 ,count(distinct case when ftotal_usd_amt = 0 then fuid end) num_0
                 ,count(distinct case when ftotal_usd_amt > 0 and ftotal_usd_amt <= 1 then fuid end) num_1
                 ,count(distinct case when ftotal_usd_amt > 1 and ftotal_usd_amt <= 2 then fuid end) num_2
                 ,count(distinct case when ftotal_usd_amt > 2 and ftotal_usd_amt <= 3 then fuid end) num_3
                 ,count(distinct case when ftotal_usd_amt > 3 and ftotal_usd_amt <= 4 then fuid end) num_4
                 ,count(distinct case when ftotal_usd_amt > 4 and ftotal_usd_amt <= 5 then fuid end) num_5
                 ,count(distinct case when ftotal_usd_amt > 5 and ftotal_usd_amt <= 6 then fuid end) num_6
                 ,count(distinct case when ftotal_usd_amt > 6 and ftotal_usd_amt <= 7 then fuid end) num_7
                 ,count(distinct case when ftotal_usd_amt > 7 and ftotal_usd_amt <= 8 then fuid end) num_8
                 ,count(distinct case when ftotal_usd_amt > 8 and ftotal_usd_amt <= 9 then fuid end) num_9
                 ,count(distinct case when ftotal_usd_amt > 9 and ftotal_usd_amt <= 10 then fuid end) num_10
                 ,count(distinct case when ftotal_usd_amt > 10 and ftotal_usd_amt <= 15 then fuid end) num_15
                 ,count(distinct case when ftotal_usd_amt > 15 then fuid end) num_m15
            from work.bud_user_paycnt_dis_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取付费相关指标
            drop table if exists work.bud_user_paycnt_dis_tmp_4_%(statdatenum)s;
          create table work.bud_user_paycnt_dis_tmp_4_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,coalesce(fpay_cnt,0) fpay_cnt
                 ,coalesce(ftotal_usd_amt,0) ftotal_usd_amt
            from dim.user_pay_day t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg1
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fuid
                 ,sum(fpay_cnt) fpay_cnt
                 ,sum(ftotal_usd_amt) ftotal_usd_amt
            from work.bud_user_paycnt_dis_tmp_4_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fuid
         """

        # 组合
        hql = (
            """ drop table if exists work.bud_user_paycnt_dis_tmp_%(statdatenum)s;
          create table work.bud_user_paycnt_dis_tmp_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg1
        hql = """
        insert into table bud_dm.bud_user_paycnt_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'pay_user' fuser_type
                 ,count(distinct case when fpay_cnt = 0 then fuid end) num_0
                 ,count(distinct case when fpay_cnt = 1 then fuid end) num_1
                 ,count(distinct case when fpay_cnt = 2 then fuid end) num_2
                 ,count(distinct case when fpay_cnt >= 3 and fpay_cnt <= 5 then fuid end) num_3
                 ,count(distinct case when fpay_cnt >= 6 and fpay_cnt <= 10 then fuid end) num_6
                 ,count(distinct case when fpay_cnt >= 11 and fpay_cnt <= 15 then fuid end) num_11
                 ,count(distinct case when fpay_cnt >= 16 then fuid end) num_16
            from work.bud_user_paycnt_dis_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总reg1
        hql = """
        insert into table bud_dm.bud_user_payincome_dis partition(dt='%(statdate)s')
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'pay_user' fuser_type
                 ,count(distinct case when ftotal_usd_amt = 0 then fuid end) num_0
                 ,count(distinct case when ftotal_usd_amt > 0 and ftotal_usd_amt <= 1 then fuid end) num_1
                 ,count(distinct case when ftotal_usd_amt > 1 and ftotal_usd_amt <= 2 then fuid end) num_2
                 ,count(distinct case when ftotal_usd_amt > 2 and ftotal_usd_amt <= 3 then fuid end) num_3
                 ,count(distinct case when ftotal_usd_amt > 3 and ftotal_usd_amt <= 4 then fuid end) num_4
                 ,count(distinct case when ftotal_usd_amt > 4 and ftotal_usd_amt <= 5 then fuid end) num_5
                 ,count(distinct case when ftotal_usd_amt > 5 and ftotal_usd_amt <= 6 then fuid end) num_6
                 ,count(distinct case when ftotal_usd_amt > 6 and ftotal_usd_amt <= 7 then fuid end) num_7
                 ,count(distinct case when ftotal_usd_amt > 7 and ftotal_usd_amt <= 8 then fuid end) num_8
                 ,count(distinct case when ftotal_usd_amt > 8 and ftotal_usd_amt <= 9 then fuid end) num_9
                 ,count(distinct case when ftotal_usd_amt > 9 and ftotal_usd_amt <= 10 then fuid end) num_10
                 ,count(distinct case when ftotal_usd_amt > 10 and ftotal_usd_amt <= 15 then fuid end) num_15
                 ,count(distinct case when ftotal_usd_amt > 15 then fuid end) num_m15
            from work.bud_user_paycnt_dis_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_paycnt_dis_tmp_%(statdatenum)s;
                 drop table if exists work.bud_user_paycnt_dis_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_paycnt_dis_tmp_2_%(statdatenum)s;
                 drop table if exists work.bud_user_paycnt_dis_tmp_3_%(statdatenum)s;
                 drop table if exists work.bud_user_paycnt_dis_tmp_4_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_paycnt_dis(sys.argv[1:])
a()
