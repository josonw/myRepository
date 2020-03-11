#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gameworld_balance_data(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists analysis.gameworld_balance_fct
        (
            fdate         date,
            fgamefsk      bigint,
            fplatformfsk  bigint,
            fbalance      decimal(20,0),
            fpaybalance   decimal(20,0),
            fusernum      bigint,
            fpayusernum   bigint
        )
        partitioned by (dt date);
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        # 依赖的源表最早是2015-05-31的，早于这个日期从oracle同步
        if self.stat_date < '2015-05-31':
            return -1

        # 如果统计日期不是月底，退出
        if self.stat_date != PublicFunc.last_day(self.stat_date):
            return -1

        self.hq.debug = 0

        args_dic = {
            "ld_month_end": self.stat_date
        }

        hql = """
        insert overwrite table analysis.gameworld_balance_fct
        partition(dt = "%(ld_month_end)s")
        select "%(ld_month_end)s" fdate,
            ta.fgamefsk, ta.fplatformfsk,
            ta.fbalance,
            tb.fpaybalance,
            ta.fusernum,
            tb.fpayusernum
        from (select * from analysis.gameworld_balance_fct_1 where dt = "%(ld_month_end)s" ) ta
        left join
        (select * from analysis.gameworld_balance_fct_2 where dt = "%(ld_month_end)s") tb
        on ta.fgamefsk = tb.fgamefsk and ta.fplatformfsk = tb.fplatformfsk;
        """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

if __name__ == '__main__':
    a = agg_gameworld_balance_data()
    a()
