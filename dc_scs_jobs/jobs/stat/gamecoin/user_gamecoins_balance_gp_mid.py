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
用户游戏币结余中间表，key是fbpid, fuid，取最后一条记录
该脚本专门用作财务递延月报用，每月1日跑一次即可
"""


class user_gamecoins_balance_gp_mid(BaseStat):

    def create_tab(self):
        # 建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create external table if not exists stage.user_gamecoins_balance_gp_mid
        (
            fdate               string,
            fbpid               string,
            fuid                bigint,
            user_gamecoins_num  decimal(20,0)
        )
        partitioned by (dt date)
        stored as orc
        location '/dw/stage/user_gamecoins_balance_gp_mid'
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        # 这个表hive最早的数据是 2015-05-31 日的，早于等于这个时间的直接不计算
        if self.stat_date <= '2015-05-31':
            return -1

        # 如果统计日期不是月底，退出
        if self.stat_date != PublicFunc.last_day(self.stat_date):
            return -1

        self.hq.debug = 0

        args_dic = {
            "ld_month_end": self.stat_date,
            "ld_month_begin": PublicFunc.trunc(self.stat_date),
            "ld_month_end_last": PublicFunc.add_days(PublicFunc.trunc(self.stat_date), -1)
        }

        res = 0

        hql = "set mapreduce.reduce.shuffle.input.buffer.percent=0.3;"

        # 最后筛选时，条件里不可添加user_gamecoins_num >= 0
        # 因为会根据需要关联gpv后再筛选一次
        hql += """
        insert overwrite table stage.user_gamecoins_balance_gp_mid
        partition(dt="%(ld_month_end)s")
        select fdate, fbpid, fuid, user_gamecoins_num
        from
        (
            select fdate, fbpid, fuid, user_gamecoins_num,
                row_number() over (partition by fbpid, fuid order by fdate desc, user_gamecoins_num desc) rown
            from
            (
                select fdate, fbpid, fuid, user_gamecoins_num
                from stage.user_gamecoins_balance_gp_mid
                where dt = "%(ld_month_end_last)s"

                union all

                select fdate, fbpid, fuid, user_gamecoins_num
                from stage.pb_gamecoins_stream_mid
                where dt >= "%(ld_month_begin)s" and dt <= "%(ld_month_end)s"
            ) t
        ) tt
        where rown = 1;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


if __name__ == '__main__':
    a = user_gamecoins_balance_gp_mid()
    a()
