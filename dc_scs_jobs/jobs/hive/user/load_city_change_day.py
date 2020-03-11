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


class load_city_change_day(BaseStatModel):
    def create_tab(self):

        hql = """--用户城市切换信息表
        create table if not exists dim.city_change_day (
               fbpid           varchar(50),
               fflts_at        string        comment '当日首次切换时间',
               fuid            bigint,
               fp_bpid         varchar(50)   comment '省份bpid',
               fchange_cnt     bigint        comment '切换次数'
               )comment '用户城市切换信息日表'
               partitioned by(dt string)
        stored as orc;

        create table if not exists dim.city_change (
               fbpid           varchar(50),
               fflts_at        string        comment '当日首次切换时间',
               fuid            bigint,
               fp_bpid         varchar(50)   comment '省份bpid'
               )comment '用户城市切换信息全量表'
               partitioned by(dt string)
        stored as orc;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.city_change_day
               partition(dt='%(statdate)s')
            select fbpid
                   ,min(flts_at) fflts_at
                   ,fuid
                   ,fp_bpid
                   ,count(1) fchange_cnt
              from stage.city_change_stg t
             where t.dt = '%(statdate)s'
             group by fbpid, fuid, fp_bpid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.city_change
               partition(dt='%(statdate)s')
            select t1.fbpid
                   ,t1.fflts_at
                   ,t1.fuid
                   ,t1.fp_bpid
              from dim.city_change_day t1
              left join dim.city_change t2
                on t1.fbpid = t2.fbpid
               and t1.fuid = t2.fuid
               and t1.fp_bpid = t2.fp_bpid
               and t2.dt <= date_sub('%(statdate)s',1)
             where t1.dt = '%(statdate)s'
               and t2.fuid is null
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


# 生成统计实例
a = load_city_change_day(sys.argv[1:])
a()
