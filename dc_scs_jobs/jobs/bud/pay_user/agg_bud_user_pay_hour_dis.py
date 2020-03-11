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


class agg_bud_user_pay_hour_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_pay_hour_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fhourfsk            int           comment '时段',
               fpay_unum           bigint        comment '付费人数',
               fpay_cnt            bigint        comment '付费次数',
               fincome             decimal(20,2) comment '付费金额',
               fpay_ucnt           bigint        comment '累计时段付费人数'
               )comment '分时段用户付费'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_pay_hour_dis';
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
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fhourfsk),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fhourfsk),
                               (fgamefsk, fgame_id, fhourfsk) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fhourfsk),
                               (fgamefsk, fplatformfsk, fhallfsk, fhourfsk),
                               (fgamefsk, fplatformfsk, fhourfsk) ) """

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
        hql = """--取付费相关指标
            drop table if exists work.bud_user_pay_hour_dis_tmp_1_%(statdatenum)s;
          create table work.bud_user_pay_hour_dis_tmp_1_%(statdatenum)s as
            select  t1.fgamefsk
                   ,t1.fplatformfsk
                   ,t1.fhallfsk
                   ,t1.fgame_id
                   ,t1.fterminaltypefsk
                   ,t1.fversionfsk
                   ,t1.fhourfsk
                   ,t1.fplatform_uid
                   ,t1.fincome
                   ,t1.fpay_cnt
                   ,row_number() over(partition by fbpid, fplatform_uid, fgame_id order by fhourfsk) rown
              from (select  tt.fbpid
                           ,tt.fgamefsk
                           ,tt.fplatformfsk
                           ,tt.fhallfsk
                           ,coalesce(t2.fgame_id,%(null_int_report)d) fgame_id
                           ,tt.fterminaltypefsk
                           ,tt.fversionfsk
                           ,hour(t1.fdate)+1 fhourfsk
                           ,t1.fplatform_uid
                           ,round(sum(t1.fcoins_num * t1.frate), 6) fincome
                           ,count(1) fpay_cnt
                      from stage.payment_stream_stg t1
                      left join (select distinct forder_id, coalesce(fgame_id,cast (0 as bigint)) fgame_id from stage.user_generate_order_stg where dt = '%(statdate)s'
                                ) t2
                        on t1.forder_id = t2.forder_id
                      join dim.bpid_map_bud tt
                        on t1.fbpid = tt.fbpid
                     where t1.dt = '%(statdate)s'
                     group by  tt.fbpid
                              ,tt.fgamefsk
                              ,tt.fplatformfsk
                              ,tt.fhallfsk
                              ,t2.fgame_id
                              ,tt.fterminaltypefsk
                              ,tt.fversionfsk
                              ,hour(t1.fdate)+1
                              ,t1.fplatform_uid) t1
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        base_hql = """
          select  fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fhourfsk           --时段
                 ,count(distinct fplatform_uid) fpay_unum   --付费人数
                 ,sum(fpay_cnt) fpay_cnt           --付费次数
                 ,sum(fincome) fincome             --付费金额
                 ,0 fpay_ucnt                      --累计时段付费人数
            from work.bud_user_pay_hour_dis_tmp_1_%(statdatenum)s t1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fhourfsk
         """

        # 组合
        hql = (
            """    drop table if exists work.bud_user_pay_hour_dis_tmp_%(statdatenum)s;
                 create table work.bud_user_pay_hour_dis_tmp_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        base_hql = """
          select '%(statdate)s' fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fhourfsk           --时段
                 ,0 fpay_unum   --付费人数
                 ,0 fpay_cnt           --付费次数
                 ,0 fincome             --付费金额
                 ,count(distinct fplatform_uid) fpay_ucnt                      --累计时段付费人数
            from work.bud_user_pay_hour_dis_tmp_1_%(statdatenum)s t1
           where rown = 1
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fhourfsk """

        # 组合
        hql = (
            """    drop table if exists work.bud_user_pay_hour_dis_tmp_2_%(statdatenum)s;
                 create table work.bud_user_pay_hour_dis_tmp_2_%(statdatenum)s as """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取付费相关指标
            insert into table work.bud_user_pay_hour_dis_tmp_%(statdatenum)s
            select  t1.fgamefsk
                   ,t1.fplatformfsk
                   ,t1.fhallfsk
                   ,t1.fgame_id
                   ,t1.fterminaltypefsk
                   ,t1.fversionfsk
                   ,fhourfsk        --时段
                   ,0 fpay_unum     --付费人数
                   ,0 fpay_cnt      --付费次数
                   ,0 fincome       --付费金额
                   ,sum(fpay_ucnt) over(partition by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    order by fhourfsk rows between unbounded preceding and current row) fpay_ucnt
              from work.bud_user_pay_hour_dis_tmp_2_%(statdatenum)s t1
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 取基础数据
        hql = """--取付费相关指标
            insert overwrite table bud_dm.bud_user_pay_hour_dis
                   partition (dt = "%(statdate)s")
            select  '%(statdate)s' fdate
                   ,fgamefsk
                   ,fplatformfsk
                   ,fhallfsk
                   ,fgame_id
                   ,fterminaltypefsk
                   ,fversionfsk
                   ,fhourfsk        --时段
                   ,max(fpay_unum) fpay_unum   --付费人数
                   ,max(fpay_cnt) fpay_cnt     --付费次数
                   ,max(fincome) fincome       --付费金额
                   ,max(fpay_ucnt) fpay_ucnt   --累计时段付费人数
              from work.bud_user_pay_hour_dis_tmp_%(statdatenum)s t1
             group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                      ,fhourfsk
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_pay_hour_dis_tmp_%(statdatenum)s;
                 drop table if exists work.bud_user_pay_hour_dis_tmp_1_%(statdatenum)s;
                 drop table if exists work.bud_user_pay_hour_dis_tmp_2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_pay_hour_dis(sys.argv[1:])
a()
