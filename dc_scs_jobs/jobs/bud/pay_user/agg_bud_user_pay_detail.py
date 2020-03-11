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


class agg_bud_user_pay_detail(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_pay_detail (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuser_type          varchar(10)    comment '用户类型',
               fpay_scene          varchar(100)   comment '场景',
               fsubname            varchar(100)   comment '二级场次',
               fgsubname           varchar(100)   comment '三级场次',
               fm_id               varchar(100)   comment '支付方式id',
               fm_name             varchar(100)   comment '支付方式',
               forder_cnt          bigint         comment '下单数',
               fpay_unum           bigint         comment '付费用户数',
               fpay_cnt            bigint         comment '付费次数',
               fpay_income         decimal(20,2)  comment '付费金额',
               ff_pay_uunm         bigint         comment '首付用户数',
               ffpay_cnt           bigint         comment '首付次数',
               ffpay_income        decimal(20,2)  comment '首付金额',
               frupt_pay_unum      bigint         comment '破产(状态)付费人数',
               frupt_pay_cnt       bigint         comment '破产(状态)付费次数',
               frupt_pay_income    bigint         comment '破产(状态)付费额度',
               forder_unum         bigint         comment '下单用户数'
               )comment '付费用户明细'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_pay_detail';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fpay_scene', 'fsubname', 'fgsubname', 'fm_id', 'fm_name'],
                        'groups': [[1, 0, 0, 0, 0],
                                   [0, 1, 0, 0, 0],
                                   [0, 1, 1, 0, 0],
                                   [0, 0, 0, 1, 1],
                                   [1, 0, 0, 1, 1],
                                   [1, 1, 1, 0, 0],
                                   [1, 1, 0, 0, 0]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--取订单相关指标
            drop table if exists work.bud_user_pay_detail_tmp_%(statdatenum)s;
          create table work.bud_user_pay_detail_tmp_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t2.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.forder_id
                 ,t1.fplatform_uid
                 ,t2.fbankrupt
                 ,coalesce(t2.fpay_scene,'%(null_str_report)s') fpay_scene
                 ,coalesce(t2.fgameparty_subname,'%(null_str_report)s')  fsubname
                 ,coalesce(t2.fgameparty_gsubname,'%(null_str_report)s')  fgsubname
                 ,coalesce(t1.fm_id,'%(null_str_report)s') fm_id
                 ,coalesce(t1.fm_name,'%(null_str_report)s') fm_name
                 ,coalesce(t3.fcoins_num * t3.frate,0) fincome
                 ,case when t3.forder_id is not null then 1 else 0 end is_sus
                 ,case when t4.fplatform_uid is not null then 1 else 0 end is_first
            from stage.payment_stream_all_stg t1  --所有订单
            left join stage.user_generate_order_stg t2
              on t1.forder_id = t2.forder_id
             and t2.dt = '%(statdate)s'
            left join stage.payment_stream_stg t3  --成功订单
              on t1.forder_id = t3.forder_id
             and t3.dt = '%(statdate)s'
            left join dim.user_pay t4  --首付
              on t3.fbpid = t4.fbpid
             and t3.fplatform_uid = t4.fplatform_uid
             and t4.dt = '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'all_user' user_type
                 ,coalesce(fpay_scene,'%(null_str_group_rule)s') fpay_scene
                 ,coalesce(fsubname,'%(null_str_group_rule)s') fsubname
                 ,coalesce(fgsubname,'%(null_str_group_rule)s') fgsubname
                 ,coalesce(fm_id,'%(null_str_group_rule)s') fm_id
                 ,coalesce(fm_name,'%(null_str_group_rule)s') fm_name
                 ,count(distinct forder_id) forder_cnt                                        --下单数
                 ,count(distinct case when is_sus = 1 then fplatform_uid end) fpay_unum       --付费用户数
                 ,count(distinct case when is_sus = 1 then forder_id end) fpay_cnt            --付费次数
                 ,sum(case when is_sus = 1 then fincome end) fpay_income                      --付费金额
                 ,count(distinct case when is_first = 1 then fplatform_uid end) ff_pay_uunm   --首付用户数
                 ,count(distinct case when is_first = 1 then forder_id end) ffpay_cnt         --首付次数
                 ,sum(case when is_first = 1 then round(fincome,4) end) ffpay_income          --首付金额
                 ,count(distinct case when is_sus = 1 and fbankrupt = 1 then fplatform_uid end) frupt_pay_unum    --破产(状态)付费人数
                 ,count(distinct case when is_sus = 1 and fbankrupt = 1 then forder_id end) frupt_pay_cnt         --破产(状态)付费次数
                 ,sum(case when is_sus = 1 and fbankrupt = 1 then round(fincome,4) end) frupt_pay_income          --破产(状态)付费额度
                 ,count(distinct fplatform_uid) forder_unum                                        --下单用户数
            from work.bud_user_pay_detail_tmp_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fpay_scene
                    ,fsubname
                    ,fgsubname
                    ,fm_id
                    ,fm_name
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
         insert overwrite table bud_dm.bud_user_pay_detail
         partition( dt="%(statdate)s" )
         %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_pay_detail_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_pay_detail(sys.argv[1:])
a()
