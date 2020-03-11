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


class agg_bud_user_pay_regdt_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_pay_regdt_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               reg_data            date             comment '注册日期',
               pay_unum            bigint           comment '当日付费用户数',
               pay_cnt             bigint           comment '当日付费次数',
               pay_income          decimal(20,2)    comment '当日付费金额',
               f_pay_uunm          bigint           comment '当日首付用户数',
               fpay_cnt            bigint           comment '当日首付次数',
               fpay_income         decimal(20,2)    comment '当日首付金额'
               )comment '付费用户新增日期分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_pay_regdt_dis';
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
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, reg_data),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, reg_data),
                               (fgamefsk, fgame_id, reg_data) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, reg_data),
                               (fgamefsk, fplatformfsk, fhallfsk, reg_data),
                               (fgamefsk, fplatformfsk, reg_data) ) """

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
        hql = """--取订单相关指标
            drop table if exists work.bud_user_pay_regdt_dis_tmp_%(statdatenum)s;
          create table work.bud_user_pay_regdt_dis_tmp_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,t1.fplatform_uid
                 ,t2.dt reg_data
                 ,t1.ftotal_usd_amt
                 ,t1.fpay_cnt
                 ,case when t4.fuid is not null then 1 else 0 end is_first
            from dim.user_pay_day t1  --日付费
            left join dim.reg_user_main_additional t2  --日新增
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join dim.user_pay t4  --日首付
              on t1.fbpid = t4.fbpid
             and t1.fuid = t4.fuid
             and t4.dt = '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,reg_data            --注册日期
                 ,count(distinct fplatform_uid) pay_unum  --当日付费用户数
                 ,sum(fpay_cnt) pay_cnt                   --当日付费次数
                 ,sum(ftotal_usd_amt) pay_income          --当日付费金额
                 ,count(distinct case when is_first = 1 then fplatform_uid end) f_pay_uunm     --当日首付用户数
                 ,sum(case when is_first = 1 then fpay_cnt end) fpay_cnt                       --当日首付次数
                 ,sum(case when is_first = 1 then ftotal_usd_amt end) fpay_income              --当日首付金额
            from work.bud_user_pay_regdt_dis_tmp_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,reg_data
         """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_user_pay_regdt_dis
            partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_pay_regdt_dis_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_pay_regdt_dis(sys.argv[1:])
a()
