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

#将原脚本拆为四个，分别计算底层数据，事件维度一、二、三
#
#本脚本计算底层数据

class agg_event_detail(BaseStatModel):

    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':[],
                        'groups':[] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """--处理接口表数据
              use stage;

              insert overwrite table stage.by_event_args_stg
                partition(dt='%(statdate)s')
              select fbpid,
                     flts_at,
                     regexp_replace(fet_id, "\\u0000", ""),
                     fet_rk_id,
                     regexp_replace(farg_name, "\\0000", ""),
                     regexp_replace(farg_value, "\\0000", ""),
                     fuid,
                     fgame_id,
                     fchannel_code
                from stage.by_event_args
               where dt='%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        self.hq.exe_sql(
            "set mapreduce.input.fileinputformat.split.maxsize=64000000;")

        hql = """--建立临时表，供后面三个结果表计算使用。使用同一个表名，下次计算时重建，不支持多天同时计算
        drop table if exists work.by_event_args_stg_tmp;
        create table work.by_event_args_stg_tmp as
            select b.dt,
                   b.fet_id,
                   b.fet_rk_id,
                   b.fuid,
                   b.farg_name,
                   b.farg_value,
                   c.fgamefsk,
                   c.fplatformfsk,
                   c.fhallfsk,
                   c.fterminaltypefsk,
                   c.fversionfsk,
                   c.hallmode,
                   coalesce(b.fgame_id,%(null_int_report)d) fgame_id,
                   coalesce(cast (b.fchannel_code as bigint),%(null_int_report)d) fchannel_code
              from stage.by_event_args_stg b
              join dim.bpid_map c
                on b.fbpid=c.fbpid
             where b.dt = "%(statdate)s";

        """

        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


#生成统计实例
a = agg_event_detail(sys.argv[1:])
a()
