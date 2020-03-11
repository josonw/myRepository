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
用户付费中间表，key是fbpid, fuid，取最后一条记录
该脚本专门用作财务递延月报用，每月1日跑一次即可
"""

class user_order_gp_mid(BaseStat):
    """建立游戏维度表
    """
    def create_tab(self):
        hql = """
        create external table if not exists stage.user_order_gp_mid
        (
            fdate      date,
            forder_at  string,
            fbpid      string,
            fuid       bigint
        )
        partitioned by (dt date)
        stored as orc
        location '/dw/stage/user_order_gp_mid'
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        # 这个表hive最早的数据是 2015-05-31 日的，早于等于这个时间的直接不计算
        if self.stat_date <= '2015-05-31':
            return -1

        # 如果统计日期不是月底，退出
        if self.stat_date != PublicFunc.last_day(self.stat_date):
            return -1

        self.hq.debug = 0

        # 分别统计日的月底、月初、上月底
        args_dic = {
            "ld_month_end": self.stat_date,
            "ld_month_begin": PublicFunc.trunc(self.stat_date),
            "ld_month_end_last": PublicFunc.add_days(PublicFunc.trunc(self.stat_date), -1)
        }

        res=0

        # 每月做一次分区，每个分区都是全量
        # 做分区的目的是要回算之前月份的数据也可以处理
        hql = """
        insert overwrite table stage.user_order_gp_mid
        partition(dt="%(ld_month_end)s")
        select max(fdate), max(forder_at),
            fbpid, fuid
        from
        (
            select fbpid, fuid, fdate, forder_at
            from stage.user_order_gp_mid
            where dt = "%(ld_month_end_last)s"

            union all

            select fbpid, fuid, to_date(forder_at) as fdate, forder_at
            from stage.user_order_stg
            where dt >= "%(ld_month_begin)s" and dt <= "%(ld_month_end)s"
        ) t
        group by fbpid, fuid
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


if __name__ == '__main__':
    a = user_order_gp_mid()
    a()
