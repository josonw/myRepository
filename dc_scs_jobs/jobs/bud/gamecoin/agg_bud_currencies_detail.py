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


class agg_bud_currencies_detail(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_currencies_detail (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fcointype           varchar(50)      comment '货币类型',
               fact_type           int              comment '操作类型：1表示+(加)，2表示-(减)',
               fact_id             varchar(50)      comment '操作编号：单个游戏内每个id表达唯一意思',
               fcurren_unum        bigint           comment '货币操作人数',
               fcurren_cnt         bigint           comment '货币操作次数',
               fcurren_num         bigint           comment '货币操作数量',
               fcurren_pay_unum    bigint           comment '货币操作人数_付费',
               fcurren_pay_cnt     bigint           comment '货币操作次数_付费',
               fcurren_pay_num     bigint           comment '货币操作数量_付费'
               )comment '货币明细表'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_currencies_detail';
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
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fcointype, fact_type, fact_id),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fcointype, fact_type, fact_id),
                               (fgamefsk, fgame_id, fcointype, fact_type, fact_id) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fcointype, fact_type, fact_id),
                               (fgamefsk, fplatformfsk, fhallfsk, fcointype, fact_type, fact_id),
                               (fgamefsk, fplatformfsk, fcointype, fact_type, fact_id) ) """

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
        hql = """--取基础指标
            drop table if exists work.bud_currencies_detail_tmp_1_%(statdatenum)s;
          create table work.bud_currencies_detail_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fcurrencies_type fcointype
                 ,t1.fact_type
                 ,t1.fact_id
                 ,t1.fuid
                 ,abs(t1.fact_num) fcoin_num
                 ,case when t2.fuid is not null then 1 else 0 end is_pay  --是否付费
            from stage.pb_currencies_stream_stg t1
            left join dim.user_pay t2
              on t1.fbpid = t2.fbpid
             and t1.fuid = t2.fuid
             and t2.dt = '%(statdate)s'
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s'
             and t1.fact_type in (1, 2)  --1:in,2:out
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 汇总1
        base_hql = """
          select "%(statdate)s" fdate
                 ,fgamefsk
                 ,coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk
                 ,coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk
                 ,coalesce(fgame_id,%(null_int_group_rule)d) fgame_id
                 ,coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk
                 ,coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk
                 ,fcointype            --货币类型
                 ,fact_type            --操作类型：1表示+(加)，2表示-(减)
                 ,fact_id              --操作编号：单个游戏内每个id表达唯一意思
                 ,count(distinct fuid) fcurren_unum  --货币操作人数
                 ,count(fuid) fcurren_cnt            --货币操作次数
                 ,sum(fcoin_num) fcurren_num         --货币操作数量
                 ,count(distinct case when is_pay = 1 then fuid end) fcurren_pay_unum    --货币操作人数_付费
                 ,count(case when is_pay = 1 then fuid end) fcurren_pay_cnt              --货币操作次数_付费
                 ,sum(case when is_pay = 1 then fcoin_num end) fcurren_pay_num           --货币操作数量_付费
            from work.bud_currencies_detail_tmp_1_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fcointype
                    ,fact_type
                    ,fact_id
         """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_currencies_detail
                      partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_currencies_detail_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_currencies_detail(sys.argv[1:])
a()
