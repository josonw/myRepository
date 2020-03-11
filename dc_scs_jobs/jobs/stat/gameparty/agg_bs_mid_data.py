#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_bs_mid_data(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):

        hql = """create table if not exists stage.bs_user_mid
                (
                  fdate                date,
                  fbpid               string,
                  fuid                bigint,
                  fpname              string,
                  fsubname            string,
                  fcnt                bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {'statdate':self.stat_date}
        query.update( dates_dict )

        # 注意开启动态分区
        res = self.hq.exe_sql("""use stage;
            set hive.exec.dynamic.partition=true;
            """)
        if res != 0: return res

        hql = """INSERT overwrite TABLE stage.bs_user_mid
                partition(dt="%(statdate)s")
                SELECT "%(statdate)s" fdate,
                       fbpid,
                       fuid,
                       fpname,
                       fsubname,
                       count(1) fcnt
                FROM stage.user_gameparty_stg
                WHERE dt = '%(statdate)s'
                  AND fpalyer_cnt != 0
                  AND fmatch_id IS NOT NULL
                  AND fmatch_id != '0'
                GROUP BY fbpid,
                         fuid,
                         fpname,
                         fsubname
            """    % query
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res






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
a = agg_bs_mid_data(statDate)
a()
