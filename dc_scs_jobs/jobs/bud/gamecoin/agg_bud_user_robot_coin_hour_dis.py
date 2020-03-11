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


class agg_bud_user_robot_coin_hour_dis(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_user_robot_coin_hour_dis (
               fdate                 date,
               fgamefsk              bigint,
               fplatformfsk          bigint,
               fhallfsk              bigint,
               fsubgamefsk           bigint,
               fterminaltypefsk      bigint,
               fversionfsk           bigint,
               fhourfsk              int           comment '时段',
               fcurrencies_type      varchar(50)      comment '货币类型',
               fact_type             int              comment '操作类型：1表示+(加)，2表示-(减)',
               fact_id               varchar(50)      comment '操作编号：单个游戏内每个id表达唯一意思',
               fgcoin_unum           bigint           comment '游戏币操作人数',
               fgcoin_cnt            bigint           comment '游戏币操作次数',
               fgcoin_num            bigint           comment '游戏币操作数量'
               )comment '机器人货币分时段'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_user_robot_coin_hour_dis';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容."""
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fhourfsk', 'fcurrencies_type', 'fact_type', 'fact_id'],
                        'groups': [[1, 1, 1, 1],
                                   [1, 1, 0, 0]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--
            drop table if exists work.bud_user_robot_coin_hour_dis_tmp_1_%(statdatenum)s;
          create table work.bud_user_robot_coin_hour_dis_tmp_1_%(statdatenum)s as
          select  tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fuid
                 ,hour(t1.flts_at)+1 fhourfsk
                 ,t1.fcurrencies_type
                 ,t1.fact_type
                 ,t1.fact_id
                 ,abs(t1.fact_num) fact_num
            from stage.pb_robot_coin_stream_stg t1
            join dim.bpid_map_bud tt
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
                 ,fhourfsk           --时段
                 ,coalesce(fcurrencies_type,'%(null_str_group_rule)s') fcurrencies_type
                 ,coalesce(fact_type,%(null_int_group_rule)d) fact_type
                 ,coalesce(fact_id,'%(null_str_group_rule)s') fact_id
                 ,count(distinct fuid) fgcoin_unum
                 ,count(fuid) fgcoin_cnt
                 ,sum(fact_num) fgcoin_num
            from work.bud_user_robot_coin_hour_dis_tmp_1_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fhourfsk,fcurrencies_type,fact_type,fact_id
         """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        # 组合
        hql = """insert overwrite table bud_dm.bud_user_robot_coin_hour_dis
                      partition(dt='%(statdate)s')
                      %(sql_template)s;
         """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_user_robot_coin_hour_dis_tmp_1_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_user_robot_coin_hour_dis(sys.argv[1:])
a()
