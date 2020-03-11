#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class parquet_flex_event_stg(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0
        #numdate为当天日期，yesterday为昨天日期
        self.hql_dict.update({'numdate':self.hql_dict['ld_end'].replace('-',''),'yesterday':self.hql_dict['ld_begin'].replace('-','')})
        numdate7ago=self.hql_dict.get('ld_7dayago').replace('-','')
        self.hql_dict.update({'numdate7ago':numdate7ago})

        #DROP TABLE IF EXISTS today.parquet_flex_event_%(numdate)s;
        hql = """
        CREATE TABLE if not exists today.parquet_flex_event_%(numdate)s (
            dt string,
            bpid STRING,
            lts_at INT,
            event_id STRING,
            event_label STRING,
            kv MAP<STRING,STRING>,
            uid BIGINT,
            game_id INT
        )
        STORED AS PARQUET
        LOCATION '/dw/today/parquet_flex_event/dt=%(ld_end)s';
        """% self.hql_dict
        print hql
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        self.hq.debug = 0

        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)

        hql = """
        INSERT OVERWRITE TABLE stage.parquet_flex_event_stg PARTITION (dt='%(ld_begin)s')
        SELECT
            bpid,
            lts_at,
            event_id,
            event_label,
            kv,
            uid,
            game_id
        FROM today.parquet_flex_event_%(yesterday)s limit  100000000;
        DROP TABLE IF EXISTS today.parquet_flex_event_%(numdate7ago)s ;
        """ % self.hql_dict

        res = self.hq.exe_sql(hql)
        return res


#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话，日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取，实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
import_job = parquet_flex_event_stg(statDate)
import_job()
