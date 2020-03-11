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


class agg_bud_user_match_join_cost_detail(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_match_join_cost_detail (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fuser_type            varchar(20)      comment '用户类型',
               fparty_type           varchar(100)     comment '牌局类型',
               fpname                varchar(100)     comment '一级场次',
               fsubname              varchar(100)     comment '二级场次',
               fgsubname             varchar(100)     comment '三级场次',
               fitem_id              varchar(100)     comment '报名物品',
               funum                 bigint           comment '报名人数',
               fcnt                  bigint           comment '报名次数',
               fitem_num             bigint           comment '报名使用物品数'
               )comment '报名用户报名费明细'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_match_join_cost_detail';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fparty_type', 'fpname', 'fsubname', 'fgsubname', 'fitem_id'],
                        'groups': [[1, 1, 1, 1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--报名
            drop table if exists work.bud_user_match_join_cost_detail_tmp_1_%(statdatenum)s;
          create table work.bud_user_match_join_cost_detail_tmp_1_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,t1.fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fmatch_id
                 ,t1.fparty_type
                 ,t1.fgsubname
                 ,t1.fsubname
                 ,t1.fpname
                 ,t1.fuid
                 ,t1.fitem_id          --物品id
                 ,t1.fentry_fee        --物品数量
            from dim.join_gameparty t1
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
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
                 ,'join' fuser_type
                 ,coalesce(fparty_type,'%(null_str_group_rule)s') fparty_type        --牌局类型
                 ,coalesce(fpname,'%(null_str_group_rule)s') fpname                  --一级场次
                 ,coalesce(fsubname,'%(null_str_group_rule)s') fsubname              --二级场次
                 ,coalesce(fgsubname,'%(null_str_group_rule)s') fgsubname            --三级场次
                 ,fitem_id          --物品
                 ,count(distinct fuid) funum    --人数
                 ,count(fuid) fcnt     --次数
                 ,sum(fentry_fee) fitem_num      --物品数
            from work.bud_user_match_join_cost_detail_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fitem_id
                    ,fparty_type
                    ,fpname
                    ,fsubname
                    ,fgsubname
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """insert overwrite table bud_dm.bud_user_match_join_cost_detail
                      partition(dt='%(statdate)s')
                      %(sql_template)s """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_match_join_cost_detail_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_match_join_cost_detail(sys.argv[1:])
a()
