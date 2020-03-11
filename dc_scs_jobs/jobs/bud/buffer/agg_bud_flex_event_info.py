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


class agg_bud_flex_event_info(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.bud_flex_event_info (
               fdate               string,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               event_id            varchar(100)     comment '事件ID',
               event_label         varchar(100)     comment '事件标签',
               fevent_unum         bigint           comment '事件人数',
               fevent_cnt          bigint           comment '事件次数'
               )comment '事件ID层级'
               partitioned by(dt string)
        location '/dw/bud_dm/bud_flex_event_info';

        create table if not exists bud_dm.bud_flex_event_parm_info (
               fdate               string,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               event_id            varchar(100)     comment '事件ID',
               event_label         varchar(100)     comment '事件标签',
               event_parm          varchar(100)     comment '事件参数',
               fparm_unum          bigint           comment '事件参数人数',
               fparm_cnt           bigint           comment '事件参数次数',
               fparm_value         bigint           comment '事件参数值汇总'
               )comment '事件参数层级'
               partitioned by(dt string)
        location '/dw/bud_dm/bud_flex_event_parm_info';

        create table if not exists bud_dm.bud_flex_event_parm_value_info (
               fdate               string,
               fgamefsk            bigint,
               fplatformfsk        bigint,
               fhallfsk            bigint,
               fsubgamefsk         bigint,
               fterminaltypefsk    bigint,
               fversionfsk         bigint,
               event_id            varchar(100)     comment '事件ID',
               event_label         varchar(100)     comment '事件标签',
               event_parm          varchar(100)     comment '事件参数',
               event_parm_value    varchar(100)     comment '事件参数值',
               fparm_unum          bigint           comment '事件参数值人数',
               fparm_cnt           bigint           comment '事件参数值次数'
               )comment '事件参数值层级'
               partitioned by(dt string)
        location '/dw/bud_dm/bud_flex_event_parm_value_info';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group_1 = {'fields': ['event_id', 'event_label'],
                          'groups': [[1, 1]]}

        extend_group_2 = {'fields': ['event_id', 'event_label', 'event_parm'],
                          'groups': [[1, 1, 1]]}

        extend_group_3 = {'fields': ['event_id', 'event_label', 'event_parm', 'event_parm_value'],
                          'groups': [[1, 1, 1, 1]]}

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        # 取基础数据
        hql = """--取基础指标
            drop table if exists work.bud_flex_event_info_tmp_%(statdatenum)s;
          create table work.bud_flex_event_info_tmp_%(statdatenum)s as
          select /*+ MAPJOIN(tt) */ tt.fgamefsk
                 ,tt.fplatformfsk
                 ,tt.fhallfsk
                 ,coalesce(t.game_id,%(null_int_report)d) fgame_id
                 ,tt.fterminaltypefsk
                 ,tt.fversionfsk
                 ,tt.hallmode
                 ,t.event_id
                 ,t.event_label
                 ,t.uid
                 ,t.kv
            from stage.parquet_flex_event_stg t
            join dim.event_id_limit t1
              on t.event_id = t1.event_id
             and t1.status = 1 --事件id限制
            join dim.bpid_map tt
              on t.bpid=tt.fbpid
           where t.dt = "%(statdate)s";
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
                 ,event_id
                 ,event_label
                 ,count(distinct uid) fevent_unum
                 ,count(uid) fevent_cnt
            from work.bud_flex_event_info_tmp_%(statdatenum)s t
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,event_id
                    ,event_label
         """
        self.sql_template_build(sql=hql, extend_group=extend_group_1)

        hql = """
         insert overwrite table bud_dm.bud_flex_event_info
         partition( dt="%(statdate)s" )
         %(sql_template)s;
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
                 ,event_id
                 ,event_label
                 ,event_parm
                 ,count(distinct uid) fparm_unum
                 ,count(uid) fparm_cnt
                 ,sum(event_parm_value) fparm_value
            from work.bud_flex_event_info_tmp_%(statdatenum)s t lateral view explode(kv) kv as event_parm,event_parm_value
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,event_id
                    ,event_label
                    ,event_parm
         """
        self.sql_template_build(sql=hql, extend_group=extend_group_2)

        hql = """
         insert overwrite table bud_dm.bud_flex_event_parm_info
         partition( dt="%(statdate)s" )
         %(sql_template)s;
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
                 ,event_id
                 ,event_label
                 ,event_parm
                 ,event_parm_value
                 ,count(distinct uid) fparm_unum
                 ,count(uid) fparm_cnt
            from work.bud_flex_event_info_tmp_%(statdatenum)s t lateral view explode(kv) kv as event_parm,event_parm_value
           where hallmode = %(hallmode)s
           group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk
                    ,event_id
                    ,event_label
                    ,event_parm
                    ,event_parm_value
         """
        self.sql_template_build(sql=hql, extend_group=extend_group_3)

        hql = """
         insert overwrite table bud_dm.bud_flex_event_parm_value_info
         partition( dt="%(statdate)s" )
         %(sql_template)s;
         """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.bud_flex_event_info_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_bud_flex_event_info(sys.argv[1:])
a()
