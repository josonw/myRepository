#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_gift_finace_day(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists analysis.gift_finace_fct(
          fdate date,
          fgamefsk bigint,
          fplatformfsk bigint,
          fversionfsk bigint,
          fterminalfsk bigint,
          fdirection varchar(50),
          ftype varchar(50),
          fnum bigint,
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

              insert overwrite table analysis.gift_finace_fct partition(dt = "%(ld_begin)s")
                select "%(ld_begin)s" fdate,
                       fgamefsk,
                       fplatformfsk,
                       fversionfsk,
                       fterminalfsk,
                       case p.act_type
                         when 1 then
                          'IN'
                         when 2 then
                          'OUT'
                       end fdirection,
                       p.gift_id ftype,
                       sum(abs(p.act_num)) fnum,
                       count(distinct fuid) fusernum
                  from stage.pb_gifts_stream_stg p
                  join analysis.bpid_platform_game_ver_map b
                    on p.fbpid = b.fbpid
                 where p.dt = "%(ld_begin)s"
                   and (act_type = 1 or act_type = 2)
                 group by fgamefsk,
                          fplatformfsk,
                          fversionfsk,
                          fterminalfsk,
                          p.act_type,
                          p.gift_id;

              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

if __name__ == '__main__':
    a = agg_gift_finace_day()
    a()
