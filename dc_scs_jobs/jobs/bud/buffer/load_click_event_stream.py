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

class load_click_event_stream(BaseStatModel):
    def create_tab(self):
        """  """
        hql = """create table if not exists dim.click_event_stream
            (fbpid           varchar(50)    comment  'BPID',
             fgamefsk            bigint,
             fplatformfsk        bigint,
             fhallfsk            bigint,
             fsubgamefsk         bigint,
             fterminaltypefsk    bigint,
             fversionfsk         bigint,
             flts_at             string         comment  '点击时间',
             fuid                bigint         comment  '用户',
             fact_id             int            comment  '动作id',
             rank                bigint         comment  '排名'
            )
            partitioned by (dt string)
        stored as parquet;

        create table if not exists dim.click_event_user
            (fbpid           varchar(50)    comment  'BPID',
             fgamefsk            bigint,
             fplatformfsk        bigint,
             fhallfsk            bigint,
             fsubgamefsk         bigint,
             fterminaltypefsk    bigint,
             fversionfsk         bigint,
             fuid                bigint         comment  '用户',
             fact_id             int            comment  '动作id',
             fmin_lts            string         comment  '最早时间',
             fmax_lts            string         comment  '最晚时间',
             max_rank            bigint         comment  '最大排名',
             min_rank            bigint         comment  '最小排名'
            )
            partitioned by (dt string)
        stored as parquet;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分,统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set mapreduce.map.memory.mb=2048;set mapreduce.map.java.opts=-XX:-UseGCOverheadLimit -Xmx1700m;""")
        if res != 0:
            return res

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.click_event_stream partition (dt = '%(statdate)s' )
        select t1.fbpid,
               fgamefsk,
               fplatformfsk,
               fhallfsk,
               fgame_id fsubgamefsk,
               fterminaltypefsk,
               fversionfsk,
               flts_at,
               fuid,
               t1.fact_id,
               row_number() over(partition by fgamefsk,fplatformfsk, fuid order by t1.flts_at) rank
          from stage.click_event_stg t1
          left join dim.click_event_dim t2
            on t1.fact_id = t2.fact_id
          join dim.bpid_map tt
            on t1.fbpid=tt.fbpid
         where dt = "%(statdate)s"
           and t2.fact_id is null;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.click_event_user partition (dt = '%(statdate)s' )
        select fbpid,
               fgamefsk,
               fplatformfsk,
               fhallfsk,
               fsubgamefsk,
               fterminaltypefsk,
               fversionfsk,
               fuid,
               fact_id,
               min(flts_at) fmin_lts,
               max(flts_at) fmax_lts,
               max(rank) max_rank,
               min(rank) min_rank
          from dim.click_event_stream t
         where dt = "%(statdate)s"
         group by fbpid,
                  fgamefsk,
                  fplatformfsk,
                  fhallfsk,
                  fsubgamefsk,
                  fterminaltypefsk,
                  fversionfsk,
                  fuid,
                  fact_id;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_click_event_stream(sys.argv[1:])
a()
