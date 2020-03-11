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


class agg_bud_user_match_cost_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_match_cost_info (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fio_type              int              comment '操作类型：1-发放，2-消耗',
               fitem_id              varchar(100)     comment '物品',
               fact_id               varchar(100)     comment '途径id，区别发放消耗途径（1报名，2奖励，3轮间奖励，4取消报名，5复活',
               funum                 bigint           comment '发放消耗人数',
               fcnt                  bigint           comment '发放消耗次数',
               fitem_num             bigint           comment '发放消耗次数物品数',
               fcost                 decimal(20,2)    comment '发放消耗次数物品价值RMB',
               fapply_unum           bigint           comment '报名用户发放消耗人数',
               ffapply_unum          bigint           comment '首次报名用户发放消耗人数',
               fapply_num            bigint           comment '对应报名用户发放消耗次数',
               ffapply_num           bigint           comment '对应首次报名用户发放消耗次数'
               )comment '赛事发放消耗'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_match_cost_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fio_type', 'fitem_id', 'fact_id'],
                        'groups': [[1, 1, 1],
                                   [1, 1, 0]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_match_cost_info_tmp_1_%(statdatenum)s;
          create table work.bud_user_match_cost_info_tmp_1_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string)) fmatch_id
                 ,t1.fuid
                 ,t1.fact_id           --途径id，区别发放消耗途径（1报名，2奖励，3轮间奖励，4取消报名，5复活
                 ,t1.fitem_id          --物品id
                 ,t1.fio_type          --发放消耗
                 ,t1.fitem_num         --物品数量
                 ,t1.fcost             --物品成本RMB
                 ,t1.frank              --获奖名次：1-冠军、2-亚军、3-季军，依次类推
                 ,case when t3.fuid is not null then 1 else 0 end ffirst_apply      --是否首次报名
                 ,case when t2.fuid is not null then 1 else 0 end fjoin_flag        --报名标识
            from stage.match_economy_stg t1
            left join (select distinct fbpid, fuid
                         from stage.join_gameparty_stg t
                        where dt = '%(statdate)s') t2 --报名用户
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
            left join (select distinct fbpid, fuid
                         from stage.join_gameparty_stg t
                        where dt = '%(statdate)s' and ffirst_match = 1) t3 --首次报名用户
              on t1.fbpid = t3.fbpid
             and t1.fuid = t3.fuid
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
                 ,fio_type          --操作类型：1-发放，2-消耗
                 ,fitem_id          --物品
                 ,coalesce(fact_id ,'%(null_str_group_rule)s') fact_id      --途径（1报名，2奖励，3轮间奖励，4取消报名，5复活
                 ,count(distinct fuid) funum    --人数
                 ,count(fuid) fcnt     --次数
                 ,sum(fitem_num) fitem_num      --物品数
                 ,sum(fcost) fcost              --物品价值RMB
                 ,count(distinct case when fjoin_flag = 1 then fuid end) fapply_unum      --报名人数
                 ,count(distinct case when fjoin_flag = 1 and ffirst_apply = 1 then fuid end) ffapply_unum      --首次报名人数
                 ,count(case when fjoin_flag = 1 then fuid end) fapply_num      --报名人次
                 ,count(case when fjoin_flag = 1 and ffirst_apply = 1 then fuid end) ffapply_num      --首次报名人次
            from work.bud_user_match_cost_info_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    , fio_type, fitem_id,fact_id
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """insert overwrite table bud_dm.bud_user_match_cost_info
                      partition(dt='%(statdate)s')
                      %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_match_cost_info_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_match_cost_info(sys.argv[1:])
a()
