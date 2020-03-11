#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class load_key_account_mid(BaseStat):
    """建立维度表
    """
    def create_tab(self):
        hql = """
        create external table if not exists stage.user_key_account_mid
        (
            fdate           date,
            fbpid           varchar(32),
            fpayment        bigint,
            fis_lost        int,
            fis_sent        int,
            fis_back        int
        )
        partitioned by (dt date)
        stored as orc
        location '/dw/stage/user_key_account_mid'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        # self.hq.debug = 0
        dates_dict = PublicFunc.date_define( self.stat_date )
        query = {}
        query.update( dates_dict )
        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """ % query)
        if res != 0: return res

        # 大客户中间表
        hql ="""
        insert into stage.user_key_account_mid
          (fdate, fbpid, fuid, fpayment)
          select fdate, fbpid, fuid, fusd / 0.157176 fpayment
            from stage.payment_stream_all
           where fdate >= '%(ld_daybegin)s'
             and fdate < '%(ld_dayend)s'
             and fbpid in (select fbpid
                             from analysis.bpid_platform_game_ver_map
                            where fgamefsk = 4125815876
                              and fusd / 0.157176 >= 100)
        """ % query
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
a = load_key_account_mid(statDate)
a()
