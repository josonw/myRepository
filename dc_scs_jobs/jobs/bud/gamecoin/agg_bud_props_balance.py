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


class agg_bud_props_balance(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_props_balance (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               ftype               varchar(50)    comment '道具编号',
               fprops_num          bigint         comment '道具所有用户结余',
               fprops_act_num      bigint         comment '道具活跃用户结余'
               )comment '道具结余'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_props_balance';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fprop_id'],
                        'groups': [[1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--取基础指标,剔除存量小于0的数据
            drop table if exists work.bud_props_balance_tmp_1_%(statdatenum)s;
          create table work.bud_props_balance_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(a.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fprop_id
                 ,t1.fuid
                 ,t1.fprops_num
                 ,case when t2.fuid is not null then 1 else 0 end is_act  --是否活跃
            from dim.user_props_balance t1
            left join dim.user_props_balance_day a
              on t1.fbpid = a.fbpid
             and t1.fuid = a.fuid
             and t1.fprop_id = a.fprop_id
             and a.dt = "%(statdate)s"
            left join (select distinct fbpid, fuid
                         from dim.user_act
                        where dt = "%(statdate)s"
                      ) t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and t1.fprops_num >=0;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fprop_id
                 ,sum(fprops_num) fprops_num                                       --所有用户结余
                 ,sum(case when is_act = 1 then fprops_num end) fprops_act_num     --活跃用户结余
            from work.bud_props_balance_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fprop_id
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """insert overwrite table bud_dm.bud_props_balance partition(dt='%(statdate)s')
             %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_props_balance_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_props_balance(sys.argv[1:])
a()
