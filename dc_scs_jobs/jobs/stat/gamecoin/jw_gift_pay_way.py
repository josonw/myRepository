#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class jw_gift_pay_way(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists analysis.gift_pay_way_fct(
          fdate date,
          fgamefsk bigint,
          fplatformfsk bigint,
          fversionfsk bigint,
          fterminalfsk bigint,
          gift_id string,
          c_type int,
          fcnt bigint,
          fusernum int)
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

        hql = """
              use stage;

              insert overwrite table analysis.gift_pay_way_fct partition(dt = "%(ld_begin)s")
                select "%(ld_begin)s" fdate,
                       fgamefsk,
                       fplatformfsk,
                       fversionfsk,
                       fterminalfsk,
                       p.gift_id,
                       p.c_type,
                       count(1) fcnt,
                       count(distinct p.fuid) fusernum
                  from stage.pb_gifts_stream_stg p
                  join analysis.bpid_platform_game_ver_map b
                    on p.fbpid = b.fbpid
                 where p.dt = "%(ld_begin)s"
                 group by fgamefsk,
                          fplatformfsk,
                          fversionfsk,
                          fterminalfsk,
                          p.gift_id,
                          p.c_type;

              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


if __name__ == '__main__':
    a = jw_gift_pay_way()
    a()
