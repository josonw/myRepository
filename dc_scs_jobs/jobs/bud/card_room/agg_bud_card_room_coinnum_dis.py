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


class agg_bud_card_room_coinnum_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_card_room_coinnum_dis (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fuser_type          varchar(10)    comment '约牌房：pa_room，棋牌室：card_room，代理商：fpartner',
               fp_num              decimal(20,2)  comment '商品数量',
               fpay_unum           bigint         comment '付费用户数',
               fpay_num            bigint         comment '付费次数'
               )comment '代理商购买房卡数量分布'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_card_room_coinnum_dis';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fp_num'],
                        'groups': [[1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--报名用户
            drop table if exists work.bud_card_room_coinnum_dis_tmp_1_%(statdatenum)s;
          create table work.bud_card_room_coinnum_dis_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t2.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fuid
                 ,t1.fplatform_uid
                 ,t1.fp_num
                 ,t1.forder_id
                 ,tt.hallmode
            from stage.payment_stream_stg t1  --成功订单
            left join stage.user_generate_order_stg t2
              on t1.forder_id = t2.forder_id
             and t2.dt = '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and (t1.fproduct_id = '6757' or t1.fproduct_name like '%%房卡%%')
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总 --报名用户
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,'fpartner' fuser_type
                 ,fp_num
                 ,count(distinct fuid) fpay_unum
                 ,count(distinct forder_id) fpay_num
            from work.bud_card_room_coinnum_dis_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fp_num
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
         insert overwrite table bud_dm.bud_card_room_coinnum_dis
         partition( dt="%(statdate)s" )
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_card_room_coinnum_dis_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_card_room_coinnum_dis(sys.argv[1:])
a()
