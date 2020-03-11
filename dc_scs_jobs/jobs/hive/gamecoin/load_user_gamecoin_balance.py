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

class load_user_gamecoin_balance(BaseStatModel):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists dim.user_gamecoin_balance
        (
            fdate               string,
            fbpid               varchar(50),         --BPID
            fuid                bigint,
            user_gamecoins_num  decimal(20,0)
        )
        partitioned by (dt date)
        stored as orc;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        # 不加user_gamecoins_num >= 0条件，加了每天的量会少200w左右
        # 但是加了之后，这个表可用的地方就非常有限了

        #res = self.sql_exe("""set mapreduce.job.queuename= %s; """%self.__class__.__name__)
        #if res != 0: return res

        hql = """
        insert overwrite table dim.user_gamecoin_balance
        partition(dt="%(statdate)s")
        select fdate, fbpid, fuid, user_gamecoins_num
        from
        (
            select fdate, fbpid, fuid, user_gamecoins_num,
                row_number() over(partition by fbpid, fuid order by fdate desc, user_gamecoins_num desc) rown
            from
            (
                select fdate, fbpid, fuid, user_gamecoins_num
                from dim.user_gamecoin_balance
                where dt = date_sub("%(statdate)s", 1)

                union all

                select fdate,fbpid, fuid, fgamecoins user_gamecoins_num
                from dim.user_gamecoin_balance_day p
                where dt = "%(statdate)s"
            ) t
        ) tt
        where rown = 1;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



a = load_user_gamecoin_balance(sys.argv[1:])
a()
