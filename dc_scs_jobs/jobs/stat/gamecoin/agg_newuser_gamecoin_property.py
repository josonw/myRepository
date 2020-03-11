#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_newuser_gamecoin_property(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
              create table if not exists analysis.user_new_gamecoin_balance(
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fnum bigint,
                fusernum int,
                fname varchar(50)
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
            "ld_begin": self.stat_date,
            "num_date": self.stat_date.replace("-", "")
        }

        #
        hql = """
              use stage;

              drop table if exists stage.newuser_gamecoins_tmp_%(num_date)s;

              create table stage.newuser_gamecoins_tmp_%(num_date)s as
              select p.fbpid, p.fuid, p.user_gamecoins_num
                from stage.pb_gamecoins_stream_mid p
                join stage.user_dim u
                  on p.fbpid = u.fbpid
                 and p.fuid = u.fuid
                 and p.dt = "%(ld_begin)s"
                 and u.dt = "%(ld_begin)s"

              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
          insert overwrite table analysis.user_new_gamecoin_balance partition(dt = "%(ld_begin)s")
          select "%(ld_begin)s" fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 fnum,
                 count(fuid),
                 fname
            from (select fgamefsk,
                         fplatformfsk,
                         fversionfsk,
                         fterminalfsk,
                         n.fuid,
                         case
                           when n.user_gamecoins_num <= 0 then
                            0
                           when n.user_gamecoins_num >= 1 and
                                n.user_gamecoins_num < 5000 then
                            1
                           when n.user_gamecoins_num >= 5000 and
                                n.user_gamecoins_num < 10000 then
                            5000
                           when n.user_gamecoins_num >= 10000 and
                                n.user_gamecoins_num < 50000 then
                            10000
                           when n.user_gamecoins_num >= 50000 and
                                n.user_gamecoins_num < 100000 then
                            50000
                           when n.user_gamecoins_num >= 100000 and
                                n.user_gamecoins_num < 500000 then
                            100000
                           when n.user_gamecoins_num >= 500000 and
                                n.user_gamecoins_num < 1000000 then
                            500000
                           when n.user_gamecoins_num >= 1000000 and
                                n.user_gamecoins_num < 5000000 then
                            1000000
                           when n.user_gamecoins_num >= 5000000 and
                                n.user_gamecoins_num < 10000000 then
                            5000000
                           when n.user_gamecoins_num >= 10000000 and
                                n.user_gamecoins_num < 50000000 then
                            10000000
                           when n.user_gamecoins_num >= 50000000 and
                                n.user_gamecoins_num < 100000000 then
                            50000000
                           when n.user_gamecoins_num >= 100000000 and
                                n.user_gamecoins_num < 1000000000 then
                            100000000
                           else
                            1000000000
                         end fnum,
                         case
                           when n.user_gamecoins_num <= 0 then
                            '0'
                           when n.user_gamecoins_num >= 1 and
                                n.user_gamecoins_num < 5000 then
                            '1-5000'
                           when n.user_gamecoins_num >= 5000 and
                                n.user_gamecoins_num < 10000 then
                            '5000-1万'
                           when n.user_gamecoins_num >= 10000 and
                                n.user_gamecoins_num < 50000 then
                            '1万-5万'
                           when n.user_gamecoins_num >= 50000 and
                                n.user_gamecoins_num < 100000 then
                            '5万-10万'
                           when n.user_gamecoins_num >= 100000 and
                                n.user_gamecoins_num < 500000 then
                            '10万-50万'
                           when n.user_gamecoins_num >= 500000 and
                                n.user_gamecoins_num < 1000000 then
                            '50万-100万'
                           when n.user_gamecoins_num >= 1000000 and
                                n.user_gamecoins_num < 5000000 then
                            '100万-500万'
                           when n.user_gamecoins_num >= 5000000 and
                                n.user_gamecoins_num < 10000000 then
                            '500万-1000万'
                           when n.user_gamecoins_num >= 10000000 and
                                n.user_gamecoins_num < 50000000 then
                            '1000万-5000万'
                           when n.user_gamecoins_num >= 50000000 and
                                n.user_gamecoins_num < 100000000 then
                            '5000万-1亿'
                           when n.user_gamecoins_num >= 100000000 and
                                n.user_gamecoins_num < 1000000000 then
                            '1亿-10亿'
                           else
                            '10亿+'
                         end fname
                    from stage.newuser_gamecoins_tmp_%(num_date)s n
                    join analysis.bpid_platform_game_ver_map b
                      on n.fbpid = b.fbpid
                  ) t
           group by fgamefsk,
                    fplatformfsk,
                    fversionfsk,
                    fterminalfsk,
                    fnum,
                    fname;
              """ % args_dic

        hql += """
        drop table if exists stage.newuser_gamecoins_tmp_%(num_date)s;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
        return res


if __name__ == '__main__':
    a = agg_newuser_gamecoin_property()
    a()
