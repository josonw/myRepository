#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_user_order_mid(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create external table if not exists stage.user_order_mid
        (
            fdate              date,
            fgamefsk           bigint,
            fplatformfsk       bigint,
            fversionfsk        bigint,
            fterminalfsk       bigint,
            fuid               bigint
        )
        partitioned by (dt date);
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        self.hq.debug = 0

        args_dic = {
            "ld_begin": self.stat_date
        }

        #
        hql = """
        insert overwrite table stage.user_order_mid partition(dt='%(ld_begin)s')
        select ta.fdate, ta.fgamefsk, ta.fplatformfsk, ta.fversionfsk, ta.fterminalfsk, ta.fuid
        from
        (
            select min(forder_at) as fdate, fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fuid
            from user_order_stg a
            join analysis.bpid_platform_game_ver_map b
            on a.fbpid = b.fbpid
            where dt = "%(ld_begin)s"
            group by fgamefsk, fplatformfsk, fversionfsk, fterminalfsk, fuid
        ) ta
        left join user_order_mid tb
            on ta.fuid = tb.fuid
            and ta.fgamefsk = tb.fgamefsk
            and ta.fplatformfsk = tb.fplatformfsk
            and ta.fversionfsk = tb.fversionfsk
            and ta.fterminalfsk = tb.fterminalfsk
        where tb.fuid is null;
        """ % args_dic

        res = 0
        # res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

if __name__ == '__main__':
    a = agg_user_order_mid()
    a()
