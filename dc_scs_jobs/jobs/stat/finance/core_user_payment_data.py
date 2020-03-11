#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


"""
支付核心数据，只运算核心数据。
原先的运算脚本太过复杂运算时长较久，所有需要搞这个。
"""

class core_user_payment_fct(BaseStat):

    def create_tab(self):
        hql = """
        create table if not exists analysis.user_payment_fct_core
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fversionfsk                 bigint,
            fterminalfsk                bigint,
            fpayusercnt                 bigint,
            fpaycnt                     bigint,
            fincome                     decimal(38,2)
        )
        partitioned by (dt date)
        """
        result = self.exe_hql(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []

        hql = """
        insert overwrite table analysis.user_payment_fct_core
        partition (dt = "%(stat_date)s")
        select "%(stat_date)s" fdate,fgamefsk,fplatformfsk,fversionfsk,fterminalfsk,
            count(distinct fplatform_uid)                fpayusercnt,
            count(1)                    fpaycnt,
            round(sum(fcoins_num * frate), 2)                    fincome
        from stage.payment_stream_stg a
        join analysis.bpid_platform_game_ver_map b
        on a.fbpid=b.fbpid
        where dt = "%(stat_date)s"
        group by fgamefsk,fplatformfsk,fversionfsk,fterminalfsk
        """ % self.hql_dict

        hql_list.append( hql )

        result = self.exe_hql_list(hql_list)
        return result


if __name__ == "__main__":

    stat_date = ''
    if len(sys.argv) == 1:
        #没有输入参数的话，日期默认取昨天
        stat_date = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
    else:
        #从输入参数取，实际是第几个就取第几个
        args = sys.argv[1].split(',')
        stat_date = args[0]

    #生成统计实例
    a = core_user_payment_fct(stat_date)
    a()
