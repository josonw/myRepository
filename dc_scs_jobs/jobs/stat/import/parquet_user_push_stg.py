#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class parquet_user_push_stg(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        #numdate为当天日期，yesterday为昨天日期
        self.hql_dict.update({'numdate':self.hql_dict['ld_end'].replace('-',''),'yesterday':self.hql_dict['ld_begin'].replace('-','')})
        numdate7ago=self.hql_dict.get('ld_7dayago').replace('-','')
        self.hql_dict.update({'numdate7ago':numdate7ago})


        hql = """
        CREATE TABLE if not exists today.parquet_user_push_%(numdate)s (
            dt string,
			bpid string,
			appid string,
			uid bigint,
			token string,
			lts_at string,
			login_at string,
			signup_at string,
			is_open int,
			version_info string,
			user_gamecoins bigint,
			m_dtype string,
			entrance_id int,
			is_paid int,
			vip_level int,
			gender int,
			channel_code string,
			party_num bigint,
			pay_num float,
			party_num_total bigint,
			pay_num_total float,
			push_platform string
        )
        STORED AS PARQUET
        LOCATION '/dw/today/parquet_user_push/dt=%(ld_end)s';
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
        INSERT OVERWRITE TABLE stage.parquet_user_push_stg PARTITION (dt='%(ld_begin)s')
        select bpid,
			appid,
			uid,
			token,
			lts_at,
			login_at,
			signup_at,
			is_open,
			version_info,
			user_gamecoins,
			m_dtype,
			entrance_id,
			is_paid,
			vip_level,
			gender,
			channel_code,
			party_num,
			pay_num,
			party_num_total,
			pay_num_total,
			push_platform
        FROM today.parquet_user_push_%(yesterday)s;
        DROP TABLE IF EXISTS today.parquet_user_push_%(numdate7ago)s;
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
import_job = parquet_user_push_stg(statDate)
import_job()
