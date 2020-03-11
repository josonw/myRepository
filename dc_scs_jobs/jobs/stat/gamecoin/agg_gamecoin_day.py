#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gamecoin_day(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create external table if not exists stage.pb_gamecoins_stream_mid
        (
            fdate date,
            fbpid string,
            fuid bigint,
            user_gamecoins_num bigint
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
        use stage;

        insert overwrite table stage.pb_gamecoins_stream_mid
        partition(dt='%(ld_begin)s')
        select fdate, fbpid, fuid, user_gamecoins_num
        from
        (
            select lts_at fdate,
                fbpid,
                fuid,
                user_gamecoins_num,
                row_number() over(partition by fbpid, fuid order by lts_at desc, nvl(fseq_no,0) desc, user_gamecoins_num desc) rown
            from stage.pb_gamecoins_stream_stg
            where dt = "%(ld_begin)s"
        ) t
        where rown = 1;
        """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


if __name__ == '__main__':
    a = agg_gamecoin_day()
    a()
