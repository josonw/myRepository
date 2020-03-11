#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class payuser_game_coin_finace_day(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists analysis.dc_payuser_coin_stream(
          fsdate date,
          fedate date,
          fgamefsk bigint,
          fplatformfsk bigint,
          fversionfsk bigint,
          fterminalfsk bigint,
          fcointype varchar(50),
          fdirection varchar(50),
          ftype varchar(50),
          fnum bigint,
          fpayusernum int
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
          "ld_begin": PublicFunc.trunc(self.stat_date),
          "ld_end": PublicFunc.add_days(PublicFunc.trunc(self.stat_date, 'nm'), -1),
          "num_date": self.stat_date.replace("-", "")
        }

        hql = """
              use stage;

              drop table if exists stage.coin_stream_tmp_%(num_date)s;

              create table stage.coin_stream_tmp_%(num_date)s as
              select fbpid,
                     fuid,
                     'GAMECOIN' fcointype,
                     act_type fdirection,
                     act_id fact_id,
                     sum(abs(act_num)) fnum
                from stage.pb_gamecoins_stream_stg
               where dt >= "%(ld_begin)s"
                 and dt <= "%(ld_end)s"
            group by fbpid,
                     fuid,
                     act_id,
                     act_type;
              """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
              drop table if exists stage.dc_payuser_coin_stream_tmp_%(num_date)s;

              create table stage.dc_payuser_coin_stream_tmp_%(num_date)s as
              select "%(ld_begin)s" fsdate,
                     "%(ld_end)s" fedate,
                     fgamefsk,
                     fplatformfsk,
                     fversion_old fversionfsk,
                     fterminalfsk,
                     'GAMECOIN' fcointype,
                     fdirection,
                     fact_id,
                     sum(abs(fnum)) fnum
                from stage.coin_stream_tmp_%(num_date)s c
                join dim.bpid_map b
                  on c.fbpid = b.fbpid
            group by fgamefsk,
                     fplatformfsk,
                     fversion_old,
                     fterminalfsk,
                     fdirection,
                     fact_id;
              """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
                drop table if exists stage.dc_payuser_coin_stream_pay_tmp_%(num_date)s;

                create table stage.dc_payuser_coin_stream_pay_tmp_%(num_date)s as
                select "%(ld_begin)s" fsdate,
                       "%(ld_end)s" fedate,
                       fgamefsk,
                       fplatformfsk,
                       fversion_old fversionfsk,
                       fterminalfsk,
                       'GAMECOIN' fcointype,
                       fdirection,
                       fact_id,
                       sum(abs(fnum)) fpayusernum
                  from stage.coin_stream_tmp_%(num_date)s c
                  join (
                        select distinct fbpid, fuid
                          from stage.user_order_stg
                         where dt >= date_sub("%(ld_end)s", 365)
                           and dt <= "%(ld_end)s"
                       ) u
                    on c.fbpid = u.fbpid
                   and c.fuid = u.fuid
                  join dim.bpid_map b
                    on c.fbpid = b.fbpid
              group by fgamefsk,
                       fplatformfsk,
                       fversion_old,
                       fterminalfsk,
                       fdirection,
                       fact_id;
              """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
              insert overwrite table analysis.dc_payuser_coin_stream
                partition(dt = "%(ld_begin)s")
              select coalesce(a.fsdate, b.fsdate),
                     coalesce(a.fedate, b.fedate),
                     coalesce(a.fgamefsk, b.fgamefsk),
                     coalesce(a.fplatformfsk, b.fplatformfsk),
                     coalesce(a.fversionfsk, b.fversionfsk),
                     coalesce(a.fterminalfsk, b.fterminalfsk),
                     coalesce(a.fcointype, b.fcointype),
                     coalesce(a.fdirection, b.fdirection),
                     coalesce(a.fact_id, b.fact_id) fact_type,
                     nvl(a.fnum, 0) fnum,
                     nvl(b.fpayusernum, 0) fpayusernum
                from stage.dc_payuser_coin_stream_tmp_%(num_date)s a
     full outer join stage.dc_payuser_coin_stream_pay_tmp_%(num_date)s b
                  on a.fsdate = b.fsdate
                 and a.fedate = b.fedate
                 and a.fgamefsk = b.fgamefsk
                 and a.fplatformfsk = b.fplatformfsk
                 and a.fversionfsk = b.fversionfsk
                 and a.fterminalfsk = b.fterminalfsk
                 and a.fcointype = b.fcointype
                 and a.fdirection = b.fdirection
                 and a.fact_id = b.fact_id;
              """ % args_dic

        hql += """
        drop table if exists stage.coin_stream_tmp_%(num_date)s;
        drop table if exists stage.dc_payuser_coin_stream_tmp_%(num_date)s;
        drop table if exists stage.dc_payuser_coin_stream_pay_tmp_%(num_date)s;
        """  % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


if __name__ == '__main__':
    a = payuser_game_coin_finace_day()
    a()
