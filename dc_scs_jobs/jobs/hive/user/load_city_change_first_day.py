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


class load_city_change_first_day(BaseStatModel):
    def create_tab(self):

        hql = """--用户城市切换信息表
        create table if not exists dim.city_change_first_day (
               fbpid           varchar(50),
               flts_at         string        comment '时间',
               fuid            bigint,
               fp_bpid         varchar(50)   comment '省份bpid'
               )comment '首次切换信息日表'
               partitioned by(dt string)
        stored as orc;

        create table if not exists dim.city_change_first (
               fbpid           varchar(50),
               flts_at         string        comment '当日首次切换时间',
               fuid            bigint,
               fp_bpid         varchar(50)   comment '省份bpid'
               )comment '首次切换信息全量表'
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
        insert overwrite table dim.city_change_first_day
               partition(dt='%(statdate)s')
               select fbpid
                      ,flts_at
                      ,fuid
                      ,fp_bpid
                 from (select fbpid
                              ,flts_at
                              ,fuid
                              ,fp_bpid
                              ,row_number() over(partition by fp_bpid, fuid order by flts_at, fbpid) row_num
                         from stage.city_change_stg
                        where dt = "%(statdate)s"
                      ) t1
                where row_num = 1
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.city_change_first
               partition(dt='%(statdate)s')
        select fbpid
               ,flts_at
               ,fuid
               ,fp_bpid
          from (select fbpid
                       ,flts_at
                       ,fuid
                       ,fp_bpid
                       ,row_number() over(partition by fp_bpid, fuid order by flts_at, fbpid) row_num
                  from (select fbpid,flts_at,fuid,fp_bpid
                          from dim.city_change_first
                         where dt = date_sub("%(statdate)s", 1)

                         union all

                        select fbpid,flts_at,fuid,fp_bpid
                          from dim.city_change_first_day p
                         where dt = "%(statdate)s"
                       ) t
               ) tt
         where row_num = 1;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


# 生成统计实例
a = load_city_change_first_day(sys.argv[1:])
a()
