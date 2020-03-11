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
该脚本每天都需要计算一份全量的游戏币结余信息
每天都依赖昨天的量，所以必须是自依赖关系
"""

class agg_gamecoin_day_balance(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create external table if not exists stage.user_gamecoins_balance_mid
        (
            fdate               date,
            fgamefsk            bigint,
            fplatformfsk        bigint,
            fversionfsk         bigint,
            fterminalfsk        bigint,
            fuid                bigint,
            user_gamecoins_num  decimal(20,0)
        )
        partitioned by (dt date)
        stored as orc;
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

        self.hq.debug = 0

        args_dic = {
            "ld_begin": self.stat_date
        }

        res=0

        # 不加user_gamecoins_num >= 0条件，加了每天的量会少200w左右
        # 但是加了之后，这个表可用的地方就非常有限了
        hql = """
        insert overwrite table stage.user_gamecoins_balance_mid
        partition(dt="%(ld_begin)s")
        select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
            fuid, user_gamecoins_num
        from
        (
            select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk,
                fuid, user_gamecoins_num,
                row_number() over(partition by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fuid order by fdate desc, user_gamecoins_num desc) rown
            from
            (
                select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fuid, user_gamecoins_num
                from stage.user_gamecoins_balance_mid
                where dt = date_sub("%(ld_begin)s", 1)

                union all

                select fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fuid, user_gamecoins_num
                from stage.pb_gamecoins_stream_mid p
                join analysis.bpid_platform_game_ver_map b
                on p.fbpid = b.fbpid
                where dt = "%(ld_begin)s"
            ) t
        ) tt
        where rown = 1;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


if __name__ == '__main__':
    a = agg_gamecoin_day_balance()
    a()
