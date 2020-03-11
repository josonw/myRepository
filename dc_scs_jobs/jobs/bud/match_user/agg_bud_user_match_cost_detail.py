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


class agg_bud_user_match_cost_detail(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_match_cost_detail (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fparty_type           varchar(10)      comment '牌局类型',
               fpname                varchar(100)     comment '一级场次',
               fsubname              varchar(100)     comment '二级场次',
               fgsubname             varchar(100)     comment '三级场次',
               fmatch_id             varchar(100)     comment '赛事id',
               fio_type              int              comment '操作类型：1-发放，2-消耗',
               fact_id               varchar(100)     comment '途径id，区别发放消耗途径（1报名，2奖励，3轮间奖励，4取消报名，5复活',
               fitem_id              varchar(100)     comment '物品',
               funum                 bigint           comment '人数',
               fcnt                  bigint           comment '次数',
               fitem_num             bigint           comment '物品数',
               fcost                 decimal(20,2)    comment '物品价值RMB'
               )comment '赛事发放消耗明细'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_match_cost_detail';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fpname', 'fsubname', 'fgsubname', 'fmatch_id', 'fparty_type', 'fio_type', 'fitem_id', 'fact_id'],
                        'groups': [[1, 1, 1, 1, 1, 1, 1, 1],
                                   [1, 1, 1, 1, 1, 1, 1, 0]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_match_cost_detail_tmp_1_%(statdatenum)s;
          create table work.bud_user_match_cost_detail_tmp_1_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,case when t1.fsubname = '快速赛' then cast (coalesce(t1.fmatch_cfg_id,0) as string)
                  else concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string)) end fmatch_id
                 ,t1.fuid
                 ,coalesce(t2.fparty_type,'%(null_str_report)s')  fparty_type
                 ,coalesce(t2.fgsubname,'%(null_str_report)s')  fgsubname
                 ,coalesce(t2.fsubname,'%(null_str_report)s')  fsubname
                 ,coalesce(t2.fpname,'%(null_str_report)s')  fpname
                 ,coalesce(t1.fact_id,'%(null_str_report)s') fact_id           --途径id，区别发放消耗途径（1报名，2奖励，3轮间奖励，4取消报名，5复活
                 ,t1.fitem_id          --物品id
                 ,t1.fio_type          --发放消耗
                 ,t1.fitem_num         --物品数量
                 ,t1.fcost             --物品成本RMB
                 ,t1.frank              --获奖名次：1-冠军、2-亚军、3-季军，依次类推
            from stage.match_economy_stg t1
            left join dim.match_config t2
              on concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string)) = t2.fmatch_id
             and t2.dt = '%(statdate)s'
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
                 ,coalesce(fparty_type,'%(null_str_group_rule)s') fparty_type        --牌局类型
                 ,coalesce(fpname,'%(null_str_group_rule)s') fpname                  --一级场次
                 ,coalesce(fsubname,'%(null_str_group_rule)s') fsubname              --二级场次
                 ,coalesce(fgsubname,'%(null_str_group_rule)s') fgsubname            --三级场次
                 ,fmatch_id
                 ,fio_type                            --操作类型：1-发放，2-消耗
                 ,coalesce(fact_id ,'%(null_str_group_rule)s') fact_id      --途径（1报名，2奖励，3轮间奖励，4取消报名，5复活
                 ,fitem_id                            --物品
                 ,count(distinct fuid) funum          --人数
                 ,count(fuid) fcnt     --次数
                 ,sum(fitem_num) fitem_num            --物品数
                 ,sum(fcost) fcost                    --物品价值RMB
            from work.bud_user_match_cost_detail_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type,fpname,fsubname,fgsubname,fmatch_id,fio_type,fitem_id,fact_id
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """insert overwrite table bud_dm.bud_user_match_cost_detail
                      partition(dt='%(statdate)s')
                      %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_match_cost_detail_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_match_cost_detail(sys.argv[1:])
a()
