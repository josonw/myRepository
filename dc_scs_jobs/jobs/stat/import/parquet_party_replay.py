#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class parquet_party_replay(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        #numdate为当天日期，yesterday为昨天日期
        self.hql_dict.update({'numdate':self.hql_dict['ld_end'].replace('-',''),'yesterday':self.hql_dict['ld_begin'].replace('-','')})
        numdate7ago=self.hql_dict.get('ld_7dayago').replace('-','')
        self.hql_dict.update({'numdate7ago':numdate7ago})

        hql = """
        CREATE TABLE if not exists today.parquet_party_replay_%(numdate)s (
            dt string,
			game_id INT,
			lts_at INT,
			mids ARRAY <INT>,
			key STRING
        )
        STORED AS PARQUET
        LOCATION '/dw/today/avro_party_replay/dt=%(ld_end)s';
        """% self.hql_dict

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
        INSERT OVERWRITE TABLE stage.parquet_party_replay PARTITION (dt='%(ld_begin)s')
		SELECT
		  game_id,
		  lts_at,
		  mids,
		  key
        FROM today.parquet_party_replay_%(yesterday)s;
        DROP TABLE IF EXISTS today.parquet_party_replay_%(numdate7ago)s;
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
import_job = parquet_party_replay(statDate)
import_job()
