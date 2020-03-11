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


class agg_bud_user_gamecoin_circulation(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_gamecoin_circulation (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fpeer_bpid          varchar(50)    comment '对端bpid',
               fcoin_type          varchar(50)    comment '游戏币类型',
               fact_type           varchar(50)    comment '操作类型:IN\OUT',
               fact_id             varchar(32)    comment '操作编号(变化原因)',
               fcoins_unum         bigint         comment '游戏币变化用户数',
               fcoins_num          bigint         comment '游戏币变化数量',
               fcoins_cnt          bigint         comment '游戏币变化次数'
       )comment '游戏币流通数据'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_gamecoin_circulation';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        # 四组组合，共8种
        extend_group_1 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fgame_id, fterminaltypefsk, fversionfsk, fpeer_bpid, fcoin_type, fact_type, fact_id),
                               (fgamefsk, fplatformfsk, fhallfsk, fgame_id, fpeer_bpid, fcoin_type, fact_type, fact_id),
                               (fgamefsk, fgame_id, fpeer_bpid, fcoin_type, fact_type, fact_id) )
                union all """

        extend_group_2 = """
                grouping sets ((fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, fpeer_bpid, fcoin_type, fact_type, fact_id),
                               (fgamefsk, fplatformfsk, fhallfsk, fpeer_bpid, fcoin_type, fact_type, fact_id),
                               (fgamefsk, fplatformfsk, fpeer_bpid, fcoin_type, fact_type, fact_id) ) """

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
            drop table if exists work.bud_user_gamecoin_circulation_tmp_1_%(statdatenum)s;
          create table work.bud_user_gamecoin_circulation_tmp_1_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,t1.fpeer_bpid
                 ,if(t1.fcurrencies_type = '1','0',coalesce(t1.fcurrencies_type,'0')) fcoin_type
                 ,t1.fact_type
                 ,t1.fact_id
                 ,abs(t1.fact_num) fnum
                 ,t1.fuid
            from stage.currency_circulation_stg t1
            join dim.bpid_map_bud tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
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
                 ,fpeer_bpid                           --对端bpid
                 ,fcoin_type                           --游戏币类型
                 ,fact_type                            --操作类型:IN\OUT
                 ,fact_id                              --操作编号(变化原因)
                 ,count(distinct fuid) fcoins_unum     --游戏币变化用户数
                 ,sum(fnum) fcoins_num                 --游戏币变化数量
                 ,count(fuid) fcoins_cnt               --游戏币变化次数
            from work.bud_user_gamecoin_circulation_tmp_1_%(statdatenum)s t
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                 ,fpeer_bpid
                 ,fcoin_type
                 ,fact_type
                 ,fact_id
         """

        # 组合
        hql = (
            """insert overwrite table bud_dm.bud_user_gamecoin_circulation
                      partition(dt='%(statdate)s') """ +
            base_hql + """%(extend_group_1)s """ +
            base_hql + """%(extend_group_2)s """) % query

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_gamecoin_circulation_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_gamecoin_circulation(sys.argv[1:])
a()
