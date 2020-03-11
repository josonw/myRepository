#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class other_api_data_info(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """create table if not exists analysis.dc_api_data_info_fct
                (
                fbpid string,
                fapi_name string,
                frownum bigint,
                fusernum bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        # 注意开启动态分区
        res = self.hq.exe_sql("""use stage;
            set hive.exec.dynamic.partition=true;
            """)
        if res != 0: return res

        hql = """
        insert overwrite table analysis.dc_api_data_info_fct
        partition( dt='%(ld_daybegin)s' )
        select fbpid, fapi_name,count(1) frownum, count(distinct fuid) fusernum
        from (
            select fbpid, 'user_login' fapi_name, fuid
            from user_login_stg where dt='%(ld_daybegin)s'
            union all
            select fbpid, 'user_signup' fapi_name, fuid
            from user_dim where dt='%(ld_daybegin)s'
            union all
            select fbpid, 'user_gameparty' fapi_name, fuid
            from user_gameparty_stg where dt='%(ld_daybegin)s'
            union all
            select fbpid, 'pb_gamecoins_stream' fapi_name, fuid
            from pb_gamecoins_stream_stg where dt='%(ld_daybegin)s'
        ) tmp group by fbpid, fapi_name
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res


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
a = other_api_data_info(statDate)
a()
