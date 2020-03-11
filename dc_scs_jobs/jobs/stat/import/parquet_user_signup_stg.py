#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class parquet_user_signup_stg(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        #numdate为当天日期，yesterday为昨天日期
        self.hql_dict.update({'numdate':self.hql_dict['ld_end'].replace('-',''),'yesterday':self.hql_dict['ld_begin'].replace('-','')})
        numdate7ago=self.hql_dict.get('ld_7dayago').replace('-','')
        self.hql_dict.update({'numdate7ago':numdate7ago})

        hql = """
        CREATE  TABLE if not exists today.parquet_user_signup_%(numdate)s (
               dt string,
		       bpid string,
		       uid bigint,
		       platform_uid string,
		       signup_at string,
		       ip string,
		       gender int,
		       age int,
		       language string,
		       country string,
		       city string,
		       friends_num bigint,
		       appfriends_num bigint,
		       profession int,
		       entrance_id bigint,
		       version_info string,
		       channel_code string,
		       ad_code string,
		       m_dtype string,
		       m_pixel string,
		       m_imei string,
		       m_os string,
		       m_network string,
		       m_operator string,
		       mnick string,
		       mname string,
		       email string,
		       mobilesms string,
		       source_path string,
		       x_country string,
		       x_province string,
		       x_city string,
		       x_country_code string,
		       latitude string,
		       longitude string,
		       partner_info string,
		       promoter string,
		       share_key string,
		       m_imsi string,
		       cid string,
		       simulator_flag int,
		       cpu_type string
        )
        STORED AS PARQUET
        LOCATION '/dw/today/parquet_user_signup/dt=%(ld_end)s';
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
        INSERT OVERWRITE TABLE stage.parquet_user_signup_stg PARTITION (dt='%(ld_begin)s')
        select bpid,
               uid,
               platform_uid,
               signup_at,
               ip,
               gender,
               age,
               language,
               country,
               city,
               friends_num,
               appfriends_num,
               profession,
               entrance_id,
               version_info,
               channel_code,
               ad_code,
               m_dtype,
               m_pixel,
               m_imei,
               m_os,
               m_network,
               m_operator,
               mnick,
               mname,
               email,
               mobilesms,
               source_path,
               x_country,
               x_province,
               x_city,
               x_country_code,
               latitude,
               longitude,
               partner_info,
               promoter,
               share_key,
               m_imsi,
               cid,
               simulator_flag,
               cpu_type
        FROM today.parquet_user_signup_%(yesterday)s limit 100000000;
        DROP TABLE IF EXISTS today.parquet_user_signup_%(numdate7ago)s ;
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
import_job = parquet_user_signup_stg(statDate)
import_job()
