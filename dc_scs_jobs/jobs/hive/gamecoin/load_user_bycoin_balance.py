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
该脚本每天都需要计算一份全量的游戏币结余信息
每天都依赖昨天的量，所以必须是自依赖关系
"""

class load_user_bycoin_balance(BaseStatModel):

    def create_tab(self):
        #建表的时候正式执行

        hql = """
        create table if not exists dim.user_bycoin_balance
        (
            fdate               string,
            fbpid               varchar(50),         --BPID
            fuid                bigint,
            user_bycoins_num  decimal(20,0)
        )
        partitioned by (dt date)
        stored as orc;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        # 不加user_bycoins_num >= 0条件，加了每天的量会少200w左右
        # 但是加了之后，这个表可用的地方就非常有限了
        hql = """
        insert overwrite table dim.user_bycoin_balance
        partition(dt="%(statdate)s")
        select fdate, fbpid, fuid, user_bycoins_num
        from
        (
            select fdate, fbpid, fuid, user_bycoins_num,
                row_number() over(partition by fbpid, fuid order by fdate desc, user_bycoins_num desc) rown
            from
            (
                select fdate, fbpid, fuid, user_bycoins_num
                from dim.user_bycoin_balance
                where dt = date_sub("%(statdate)s", 1)

                union all

                select fdate, fbpid, fuid, fbycoins user_bycoins_num
                from dim.user_bycoin_balance_day p
                where dt = "%(statdate)s"
            ) t
        ) tt
        where rown = 1;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



a = load_user_bycoin_balance(sys.argv[1:])
a()
