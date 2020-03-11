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


class agg_bud_match_cost_minute_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_match_cost_minute_info (
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
               fminute               varchar(100)     comment '分钟',
               fio_type              int              comment '操作类型：1-发放，2-消耗',
               fact_id               varchar(100)     comment '途径id，区别发放消耗途径（1报名，2奖励，3轮间奖励，4取消报名，5复活',
               fact_desc             varchar(100)     comment '详细变化原因',
               fitem_id              varchar(100)     comment '物品',
               fitem_num             bigint           comment '物品数',
               fcost                 decimal(20,2)    comment '物品价值RMB'
               )comment '发放消耗时段人数'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_match_cost_minute_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fpname', 'fsubname', 'fgsubname', 'fparty_type', 'fminute', 'fio_type', 'fitem_id', 'fact_id', 'fact_desc'],
                        'groups': [[1, 1, 1, 1, 1, 1, 1, 1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_match_cost_minute_info_tmp_1_%(statdatenum)s;
          create table work.bud_match_cost_minute_info_tmp_1_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fuid
                 ,from_unixtime(floor(unix_timestamp(flts_at) / 300)* 300,'HH:mm') fminute
                 ,coalesce(t.fparty_type,'%(null_str_report)s')  fparty_type
                 ,coalesce(t.fgsubname,'%(null_str_report)s')  fgsubname
                 ,coalesce(t.fsubname,'%(null_str_report)s')  fsubname
                 ,coalesce(t.fpname,'%(null_str_report)s')  fpname
                 ,coalesce(t1.fact_id,'%(null_str_report)s')  fact_id     --途径id，区别发放消耗途径（1报名，2奖励，3轮间奖励，4取消报名，5复活
                 ,coalesce(t1.fact_desc,'%(null_str_report)s')  fact_desc     --详细变化原因
                 ,coalesce(t1.fitem_id,'%(null_str_report)s')  fitem_id     --物品id
                 ,t1.fio_type          --发放消耗
                 ,t1.fitem_num         --物品数量
                 ,t1.fcost             --物品成本RMB
            from stage.match_economy_stg t1
            left join dim.match_config t
              on t.fmatch_id = concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string))
             and t.dt = '%(statdate)s'
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
                 ,coalesce(fminute,'%(null_str_group_rule)s') fminute
                 ,coalesce(fio_type,%(null_int_group_rule)d) fio_type
                 ,coalesce(fact_id,'%(null_str_group_rule)s') fact_id
                 ,coalesce(fact_desc,'%(null_str_group_rule)s') fact_desc
                 ,coalesce(fitem_id,'%(null_str_group_rule)s') fitem_id
                 ,sum(fitem_num) fitem_num          --物品数
                 ,sum(fcost) fcost                  --成本
            from work.bud_match_cost_minute_info_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fparty_type,fpname,fsubname,fgsubname,fminute,fio_type,fitem_id,fact_id,fact_desc
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """insert overwrite table bud_dm.bud_match_cost_minute_info
                      partition(dt='%(statdate)s')
                      %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_match_cost_minute_info_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_match_cost_minute_info(sys.argv[1:])
a()
