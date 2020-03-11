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


class agg_xxx_user_pay_info(BaseStatModel):

    def create_tab(self):
        hql = """
        create table if not exists dcnew.xxx_user_pay_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fchannelcode        bigint,
               fuser_type          varchar(10)   comment '用户类型：ad等',
               fuser_type_id       varchar(100)  comment '用户类型id',
               fm_fsk              varchar(100)  comment '付费渠道',
               forder_cnt          bigint        comment '下单数',
               fpay_unum           bigint        comment '付费用户数',
               fpay_cnt            bigint        comment '付费次数',
               fpay_income         decimal(20,2) comment '付费金额',
               ff_pay_uunm         bigint        comment '首付用户数',
               ffpay_cnt           bigint        comment '首付次数',
               ffpay_income        decimal(20,2) comment '首付金额'
               )comment 'xxx用户付费信息表'
               partitioned by(dt date)
        location '/dw/dcnew/xxx_user_pay_info'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fuser_type', 'fuser_type_id', 'fm_fsk'],
                        'groups': [[1, 1, 1],
                                   [1, 1, 0]]
                        }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """--取当日新增活跃玩牌付费用户
                  drop table if exists work.xxx_user_pay_info_tmp_a_%(statdatenum)s;
                create table work.xxx_user_pay_info_tmp_a_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t2.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,%(null_int_report)d fchannel_code
                 ,coalesce(t5.fuser_type,'ad') fuser_type
                 ,coalesce(t5.fuser_type_id,'%(null_str_report)s') fuser_type_id
                 ,t1.forder_id
                 ,coalesce(t3.fm_id ,'%(null_str_report)s') fm_fsk
                 ,t1.fplatform_uid
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
            left join dim.xxx_user t5
              on t1.fbpid = t5.fbpid
             and t1.fuid = t5.fuid
             and t5.dt <= '%(statdate)s'
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       fuser_type,         --用户类型：ad等
                       fuser_type_id,      --用户类型id
                       coalesce(fm_fsk,'%(null_str_group_rule)s') fm_fsk, --付费渠道
                       count(distinct forder_id) forder_cnt,                                        --下单数
                       count(distinct case when is_sus = 1 then fplatform_uid end) fpay_unum,       --付费用户数
                       count(distinct case when is_sus = 1 then forder_id end) fpay_cnt,            --付费次数
                       sum(case when is_sus = 1 then fincome end) fpay_income,                      --付费金额
                       count(distinct case when is_first = 1 then fplatform_uid end) ff_pay_uunm,   --首付用户数
                       count(distinct case when is_first = 1 then forder_id end) ffpay_cnt,         --首付次数
                       sum(case when is_first = 1 then round(fincome,4) end) ffpay_income           --首付金额
                  from work.xxx_user_pay_info_tmp_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,
                       fuser_type,
                       fuser_type_id,
                       fm_fsk
        """

        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.xxx_user_pay_info
        partition( dt="%(statdate)s" )
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.xxx_user_pay_info_tmp_a_%(statdatenum)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_xxx_user_pay_info(sys.argv[1:])
a()
