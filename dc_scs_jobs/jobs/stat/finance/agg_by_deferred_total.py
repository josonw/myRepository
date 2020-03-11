#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_by_deferred_total(BaseStat):

    def create_tab(self):
        pass


    def stat(self):
        """ 重要部分，统计内容 """
        ld_date = self.hql_dict.get('ld_month_begin')
        pd_date = self.hql_dict.get('stat_date')
        next_date = PublicFunc.add_days(pd_date,1)

        if ld_date == pd_date:
            import agg_by_deferred_goods_balance
            agg_by_deferred_goods_balance.agg_by_deferred_total(pd_date)() # 递延道具

        #  每月一号跑上个月的数据， pd_date = 上个月最后一天的日期
        if self.hql_dict.get('ld_1month_after_begin') == next_date:
            import load_by_deferred_income_data
            load_by_deferred_income_data.agg_by_deferred_total(next_date)()

            import agg_by_deferred_gamecoin
            agg_by_deferred_gamecoin.agg_by_deferred_total(next_date)()     # 递延游戏币

            import agg_by_deferred_bycoin_balance
            agg_by_deferred_bycoin_balance.agg_by_deferred_total(next_date)() # 递延博雅币

        return 0



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
    a = agg_by_deferred_total(stat_date)
    a()
