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


class agg_phone_bill_coupon_exchange_all(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.phone_bill_coupon_exchange_all_all (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fcztype             varchar(32) comment '充值类型，话费充值type为0，流量充值为1',
               fsus_order          bigint      comment '成功订单数',
               fsus_user           bigint      comment '成功用户数',
               fsus_mobile         bigint      comment '成功手机号数',
               fsus_cash           decimal(10,2)      comment '成功金额'
               )comment '话费券兑换信息'
               partitioned by(dt date)
        location '/dw/bud_dm/phone_bill_coupon_exchange_all';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fcztype'],
                        'groups': [[1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.phone_bill_coupon_exchange_all_tmp_%(statdatenum)s;
          create table work.phone_bill_coupon_exchange_all_tmp_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ distinct tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,%(null_int_report)d fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,fcztype      --充值类型(话费充值type为0,流量充值为1)
                 ,fuid
                 ,fsporderid   --sp商户订单号
                 ,fordercash   --实际扣款金额
                 ,fmobile      --充值手机号码
            from stage.phone_bill_coupon_exchange_all_stg t1  --话费券兑换
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and fstatus = 2  --充值成功
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
                 ,fcztype
                 ,count(distinct fsporderid) fsus_order
                 ,count(distinct fuid) fsus_user
                 ,count(distinct fmobile) fsus_mobile
                 ,sum(fordercash) fsus_cash
            from work.phone_bill_coupon_exchange_all_tmp_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fcztype
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
            insert overwrite table bud_dm.phone_bill_coupon_exchange_all  partition(dt='%(statdate)s')
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.phone_bill_coupon_exchange_all_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_phone_bill_coupon_exchange_all(sys.argv[1:])
a()
