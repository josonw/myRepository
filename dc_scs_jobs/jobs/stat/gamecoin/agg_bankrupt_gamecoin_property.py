#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_bankrupt_gamecoin_property(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists analysis.user_bankrupt_gamecoin
        (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fversionfsk bigint,
            fterminalfsk bigint,
            fnum bigint,
            fusernum int,
            fname varchar(15)
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
            "ld_end": PublicFunc.add_days(self.stat_date, 1),
            "ld_90dayago": PublicFunc.add_days(self.stat_date, -90),
            "num_date": self.stat_date.replace("-", "")
        }

        #
        hql = """
              use stage;

              drop table if exists stage.login_gamecoins_tmp_%(num_date)s;

              create table stage.login_gamecoins_tmp_%(num_date)s as
              select fdate, fbpid, fuid, user_gamecoins_num
                from (
                      select dt fdate,
                             fbpid,
                             fuid,
                             user_gamecoins user_gamecoins_num,
                             row_number() over(partition by fbpid, fuid order by flogin_at, user_gamecoins) rown
                        from stage.user_login_stg u
                       where u.dt = "%(ld_begin)s"
                     ) t
               where rown = 1;

              drop table if exists stage.bankrupt_stream_tmp_%(num_date)s;

              create table stage.bankrupt_stream_tmp_%(num_date)s as
              select fbpid, fuid, "%(ld_begin)s" fdate, count(1) frupt_num
                from stage.user_bankrupt_stg
               where dt = "%(ld_begin)s"
            group by fbpid, fuid
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
          insert overwrite table analysis.user_bankrupt_gamecoin partition(dt = "%(ld_begin)s")
          select "%(ld_begin)s" fdate,
                 fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 fnum,
                 count(distinct fuid),
                 fname
            from (select fgamefsk,
                         fplatformfsk,
                         fversionfsk,
                         fterminalfsk,
                         l.fuid,
                         case
                           when l.user_gamecoins_num <= 0 then
                            0
                           when l.user_gamecoins_num >= 1 and
                                l.user_gamecoins_num < 5000 then
                            1
                           when l.user_gamecoins_num >= 5000 and
                                l.user_gamecoins_num < 10000 then
                            5000
                           when l.user_gamecoins_num >= 10000 and
                                l.user_gamecoins_num < 50000 then
                            10000
                           when l.user_gamecoins_num >= 50000 and
                                l.user_gamecoins_num < 100000 then
                            50000
                           when l.user_gamecoins_num >= 100000 and
                                l.user_gamecoins_num < 500000 then
                            100000
                           when l.user_gamecoins_num >= 500000 and
                                l.user_gamecoins_num < 1000000 then
                            500000
                           when l.user_gamecoins_num >= 1000000 and
                                l.user_gamecoins_num < 5000000 then
                            1000000
                           when l.user_gamecoins_num >= 5000000 and
                                l.user_gamecoins_num < 10000000 then
                            5000000
                           when l.user_gamecoins_num >= 10000000 and
                                l.user_gamecoins_num < 50000000 then
                            10000000
                           when l.user_gamecoins_num >= 50000000 and
                                l.user_gamecoins_num < 100000000 then
                            50000000
                           when l.user_gamecoins_num >= 100000000 and
                                l.user_gamecoins_num < 1000000000 then
                            100000000
                           else
                            1000000000
                         end fnum,
                         case
                           when l.user_gamecoins_num <= 0 then
                            '0'
                           when l.user_gamecoins_num >= 1 and
                                l.user_gamecoins_num < 5000 then
                            '1-5000'
                           when l.user_gamecoins_num >= 5000 and
                                l.user_gamecoins_num < 10000 then
                            '5000-1万'
                           when l.user_gamecoins_num >= 10000 and
                                l.user_gamecoins_num < 50000 then
                            '1万-5万'
                           when l.user_gamecoins_num >= 50000 and
                                l.user_gamecoins_num < 100000 then
                            '5万-10万'
                           when l.user_gamecoins_num >= 100000 and
                                l.user_gamecoins_num < 500000 then
                            '10万-50万'
                           when l.user_gamecoins_num >= 500000 and
                                l.user_gamecoins_num < 1000000 then
                            '50万-100万'
                           when l.user_gamecoins_num >= 1000000 and
                                l.user_gamecoins_num < 5000000 then
                            '100万-500万'
                           when l.user_gamecoins_num >= 5000000 and
                                l.user_gamecoins_num < 10000000 then
                            '500万-1000万'
                           when l.user_gamecoins_num >= 10000000 and
                                l.user_gamecoins_num < 50000000 then
                            '1000万-5000万'
                           when l.user_gamecoins_num >= 50000000 and
                                l.user_gamecoins_num < 100000000 then
                            '5000万-1亿'
                           when l.user_gamecoins_num >= 100000000 and
                                l.user_gamecoins_num < 1000000000 then
                            '1亿-10亿'
                           else
                            '10亿+'
                         end fname
                    from stage.login_gamecoins_tmp_%(num_date)s l
                    join stage.bankrupt_stream_tmp_%(num_date)s b
                      on l.fbpid = b.fbpid and l.fuid = b.fuid
                    join analysis.bpid_platform_game_ver_map bp
                      on l.fbpid = bp.fbpid
                   where l.fdate = "%(ld_begin)s"
                  ) t
           group by fgamefsk,
                    fplatformfsk,
                    fversionfsk,
                    fterminalfsk,
                    fnum,
                    fname;
              """ % args_dic

        hql += """
        drop table if exists stage.login_gamecoins_tmp_%(num_date)s;
        drop table if exists stage.bankrupt_stream_tmp_%(num_date)s;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res
        return res



if __name__ == '__main__':
    a = agg_bankrupt_gamecoin_property()
    a()
