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


class agg_bud_click_event_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_click_event_info (
               fdate               date,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               fpname              varchar(100)     comment '一级场次',
               fsubname            varchar(100)     comment '二级场次',
               fgsubname           varchar(100)     comment '三级场次',
               fact_id             int              comment '动作id',
               funum               bigint           comment '人数',
               fnum                bigint           comment '次数'
               )comment '点击事件'
               partitioned by(dt date)
        location '/dw/bud_dm/bud_click_event_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields': ['fact_id', 'fpname', 'fsubname', 'fgsubname'],
                        'groups': [[1, 1, 1, 1],
                                   [1, 0, 0, 0]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--点击事件
            drop table if exists work.bud_click_event_info_tmp_%(statdatenum)s;
          create table work.bud_click_event_info_tmp_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t1.fgame_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t1.fuid
                 ,t1.fact_id
                 ,t1.fgamecoins
                 ,t1.fcurrencies
                 ,t1.fitem1_num
                 ,t1.fitem2_num
                 ,t1.fitem3_num
                 ,t1.fitem4_num
                 ,t1.fitem5_num
                 ,t1.fbank_gamecoins
                 ,t1.fbank_currencies
                 ,coalesce(t1.fpname,'%(null_str_report)s')  fpname
                 ,coalesce(t1.fsubname,'%(null_str_report)s')  fsubname
                 ,coalesce(t1.fgsubname,'%(null_str_report)s')  fgsubname
                 ,t1.fvip_type
                 ,t1.fvip_level
            from stage.click_event_stg t1  --点击事件
            join dim.bpid_map tt
              on t1.fbpid = tt.fbpid
           where t1.dt = '%(statdate)s';
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
                 ,coalesce(fpname,'%(null_str_group_rule)s')  fpname
                 ,coalesce(fsubname,'%(null_str_group_rule)s')  fsubname
                 ,coalesce(fgsubname,'%(null_str_group_rule)s')  fgsubname
                 ,coalesce(fact_id, %(null_int_group_rule)s) fact_id   --动作id
                 ,count(distinct fuid) funum   --人数
                 ,count(fuid) fnum             --次数
            from work.bud_click_event_info_tmp_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,fact_id
                    ,fpname
                    ,fsubname
                    ,fgsubname
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
         insert overwrite table bud_dm.bud_click_event_info
         partition( dt="%(statdate)s" )
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_click_event_info_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_click_event_info(sys.argv[1:])
a()
