#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class by_event_base(BaseStat):

    def create_tab(self):
        # 建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create table if not exists analysis.by_event_fct
        (
          fdate date,
          fgamefsk bigint,
          fplatformfsk bigint,
          fversionfsk bigint,
          fterminalfsk bigint,
          fet_id varchar(100),
          feventcnt int,
          fusercnt int
        )
        partitioned by (dt date);

        create table if not exists analysis.by_event_args_fct
        (
          fdate date,
          fgamefsk bigint,
          fplatformfsk bigint,
          fversionfsk bigint,
          fterminalfsk bigint,
          fet_id varchar(100),
          farg_name varchar(100),
          fargvalue varchar(100),
          fargcnt int,
          fusercnt int
        )
        partitioned by (dt date);

        create table if not exists analysis.by_event_args_new_fct
        (
          fdate date,
          fgamefsk bigint,
          fplatformfsk bigint,
          fversionfsk bigint,
          fterminalfsk bigint,
          fet_id varchar(100),
          farg_name varchar(100),
          fargcnt int,
          fusercnt int,
          fargvalue_sum bigint
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

        hql = """
              use stage;

              insert overwrite table stage.by_event_args_stg
                partition(dt='%(ld_begin)s')
              select fbpid,
                     flts_at,
                     regexp_replace(fet_id, "\\u0000", ""),
                     fet_rk_id,
                     regexp_replace(farg_name, "\\0000", ""),
                     regexp_replace(farg_value, "\\0000", ""),
                     fuid,
                     fgame_id,
                     fchannel_code
                from stage.by_event_args
               where dt='%(ld_begin)s'
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        self.hq.exe_sql(
            "set mapreduce.input.fileinputformat.split.maxsize=64000000;")

        hql = """
                use stage;
                set io.sort.mb=256;
                from (
                      select b.dt,
                             b.fet_id,
                             b.fet_rk_id,
                             b.fuid,
                             b.farg_name,
                             b.farg_value,
                             gpv.fgamefsk,
                             gpv.fplatformfsk,
                             gpv.fversionfsk,
                             gpv.fterminalfsk
                        from stage.by_event_args_stg b
                        join analysis.bpid_platform_game_ver_map gpv
                          on b.fbpid = gpv.fbpid
                       where dt = "%(ld_begin)s"
                     ) t
                insert overwrite table analysis.by_event_fct partition(dt="%(ld_begin)s")
                    select dt fdate,
                           fgamefsk,
                           fplatformfsk,
                           fversionfsk,
                           fterminalfsk,
                           fet_id,
                           count(distinct fet_rk_id) feventcnt,
                           count(distinct fuid) fusercnt
                     group by dt,
                              fgamefsk,
                              fplatformfsk,
                              fversionfsk,
                              fterminalfsk,
                              fet_id
                insert overwrite table analysis.by_event_args_new_fct partition(dt="%(ld_begin)s")
                    select dt fdate,
                           fgamefsk,
                           fplatformfsk,
                           fversionfsk,
                           fterminalfsk,
                           fet_id,
                           farg_name,
                           count(farg_name) fargcnt,
                           count(distinct fuid) fusercnt,
                           sum(cast(farg_value as bigint)) fargvalue_sum
                     group by dt,
                              fgamefsk,
                              fplatformfsk,
                              fversionfsk,
                              fterminalfsk,
                              fet_id,
                              farg_name
                insert overwrite table analysis.by_event_args_fct partition(dt="%(ld_begin)s")
                    select dt fdate,
                           fgamefsk,
                           fplatformfsk,
                           fversionfsk,
                           fterminalfsk,
                           fet_id,
                           farg_name,
                           farg_value,
                           count(farg_value) fargcnt,
                           count(distinct fuid) fusercnt
                     group by dt,
                              fgamefsk,
                              fplatformfsk,
                              fversionfsk,
                              fterminalfsk,
                              fet_id,
                              farg_name,
                              farg_value;
          """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        """还原Map数目"""
        self.hq.exe_sql(
            "set mapreduce.input.fileinputformat.split.maxsize=256000000;")

        # hql = """
        #         insert overwrite table analysis.by_event_args_fct partition(dt="%(ld_begin)s")
        #             select fdate,
        #                    fgamefsk,
        #                    fplatformfsk,
        #                    fversionfsk,
        #                    fterminalfsk,
        #                    fet_id,
        #                    farg_name,
        #                    fargvalue,
        #                    fargcnt,
        #                    fusercnt
        #               from (
        #                     select dt fdate,
        #                            fgamefsk,
        #                            fplatformfsk,
        #                            fversionfsk,
        #                            fterminalfsk,
        #                            fet_id,
        #                            farg_name,
        #                            fargvalue,
        #                            fargcnt,
        #                            fusercnt,
        #                            count(distinct fargvalue) over(
        #                                partition by fgamefsk, fplatformfsk, fversionfsk,
        #                                fet_id, farg_name ) as value_count
        #                       from analysis.by_event_args_fct
        #                      where dt = "%(ld_begin)s"
        #                      group by dt,
        #                            fgamefsk,
        #                            fplatformfsk,
        #                            fversionfsk,
        #                            fterminalfsk,
        #                            fet_id,
        #                            farg_name,
        #                            fargvalue,
        #                            fargcnt,
        #                            fusercnt
        #                     ) t
        #              where value_count < 150;
        #       """ % args_dic

        hql = """
                insert overwrite table analysis.by_event_args_fct partition(dt="%(ld_begin)s")
                    select fdate,
                           fgamefsk,
                           fplatformfsk,
                           fversionfsk,
                           fterminalfsk,
                           regexp_replace(fet_id, "\\u0000", "") fet_id,
                           regexp_replace(farg_name, "\\u0000", "") farg_name,
                           regexp_replace(fargvalue, "\\u0000", "") fargvalue,
                           fargcnt,
                           fusercnt
                      from analysis.by_event_args_fct b
            left semi join (
                            select fgamefsk,
                                   fplatformfsk,
                                   fversionfsk,
                                   fterminalfsk,
                                   fet_id,
                                   farg_name,
                                   count(distinct fargvalue) value_count
                              from analysis.by_event_args_fct
                             where dt = "%(ld_begin)s"
                          group by fgamefsk,
                                   fplatformfsk,
                                   fversionfsk,
                                   fterminalfsk,
                                   fet_id,
                                   farg_name
                            ) t
                        on b.fgamefsk = t.fgamefsk
                       and b.fplatformfsk = t.fplatformfsk
                       and b.fversionfsk = t.fversionfsk
                       and b.fterminalfsk = t.fterminalfsk
                       and b.fet_id = t.fet_id
                       and b.farg_name = t.farg_name
                       and value_count < 150
                     where dt = '%(ld_begin)s'
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

if __name__ == '__main__':
    a = by_event_base()
    a()
