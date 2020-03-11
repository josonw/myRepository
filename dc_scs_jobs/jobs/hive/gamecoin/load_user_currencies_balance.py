#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel

"""
该脚本每天都需要计算一份全量的货币结余信息
每天都依赖昨天的量，所以必须是自依赖关系
"""

class load_user_currencies_balance(BaseStatModel):

    def create_tab(self):
        #建表的时候正式执行

        hql = """
        create table if not exists dim.user_currencies_balance
        (
            fdate               string,
            fbpid               varchar(50),         --BPID
            fuid                bigint,
            fcurrencies_type    varchar(10),         --用户货币类型
            fcurrencies_num     bigint               --用户货币携带
        )
        partitioned by (dt date)
        stored as orc;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        hql = """
        insert overwrite table dim.user_currencies_balance
        partition(dt="%(statdate)s")
        select fdate, fbpid, fuid, fcurrencies_type, fcurrencies_num
        from
        (
            select fdate, fbpid, fuid, fcurrencies_type, fcurrencies_num,
                row_number() over(partition by fbpid, fuid, fcurrencies_type order by fdate desc, fcurrencies_num desc) rown
            from
            (
                select fdate, fbpid, fuid, fcurrencies_type, fcurrencies_num
                from dim.user_currencies_balance
                where dt = date_sub("%(statdate)s", 1)

                union all

                select fdate, fbpid, fuid, fcurrencies_type, fcurrencies_num
                from dim.user_currencies_balance_day p
                where dt = "%(statdate)s"
            ) t
        ) tt
        where rown = 1;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



a = load_user_currencies_balance(sys.argv[1:])
a()
