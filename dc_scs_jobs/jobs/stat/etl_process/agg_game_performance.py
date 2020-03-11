#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_game_performance(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """

        create  table if not exists analysis.game_performance_fct
        (
            fdate               date,
            fgamefsk            bigint,
            fplatformfsk        bigint,
            fversionfsk         bigint,
            fterminalfsk        bigint,
            fptype              int,
            fact_type           string,
            fret_code           int,
            fret_msg            string,
            fm_operator         string,
            fm_network          string,
            fm_dtype            string,
            fm_pixel            string,
            fhour               string,
            fhour_dis           string,
            fduration           string,
            fusernum            bigint,
            fusercnt            bigint,
            fcosttime           decimal(20,4)

        )
        partitioned by (dt date)

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

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res


        hql = """
        insert overwrite table analysis.game_performance_fct
        partition( dt="%(ld_begin)s" )
        select '%(ld_begin) s' fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               fterminalfsk,
               fptype,
               fact_type,
               fret_code,
               regexp_replace(fret_msg, "\\u0000", "") fret_msg,
               fm_operator,
               fm_network,
               fm_dtype,
               fm_pixel,
               fhour,
                case
                 when fhour >= 0 and fhour < 2 then
                  '0-2时'
                 when fhour >= 2 and fhour < 4 then
                  '2-4时'
                 when fhour >= 4 and fhour < 6 then
                  '4-6时'
                 when fhour >= 6 and fhour < 8 then
                  '6-8时'
                 when fhour >= 8 and fhour < 10 then
                  '8-10时'
                 when fhour >= 10 and fhour < 12 then
                  '10-12时'
                 when fhour >= 12 and fhour < 14 then
                  '12-14时'
                 when fhour >= 14 and fhour < 16 then
                  '14-16时'
                 when fhour >= 16 and fhour < 18 then
                  '16-18时'
                 when fhour >= 18 and fhour < 20 then
                  '18-20时'
                 when fhour >= 20 and fhour < 22 then
                  '20-22时'
                 when fhour >= 22 and fhour < 24 then
                  '22-24时'
               end fhour_dis,
               case
                 when avg(fcosttime) >= 0 and avg(fcosttime) < 1 then
                  '0-1s'
                 when avg(fcosttime) >= 1 and avg(fcosttime) < 2 then
                  '1-2s'
                 when avg(fcosttime) >= 2 and avg(fcosttime) < 3 then
                  '2-3s'
                 when avg(fcosttime) >= 3 and avg(fcosttime) < 4 then
                  '3-4s'
                 when avg(fcosttime) >= 4 and avg(fcosttime) < 5 then
                  '4-5s'
                 when avg(fcosttime) >= 5 and avg(fcosttime) < 6 then
                  '5-6s'
                 when avg(fcosttime) >= 6 then
                  '6s+'
               end fduration,
               count(distinct fuid) fusernum,
               count(fuid) fusercnt,
               avg(fcosttime) fcosttime,
               fterminalsysname
          from (select fgamefsk,
                       fplatformfsk,
                       fversionfsk,
                       fterminalfsk,
                       fterminalsysname,
                       m.fuid,
                       fptype,
                       fact_type,
                       fret_code,
                       fret_msg,
                       case
                         when fstart_time = '1970-01-01 00:00:00' then
                          0
                         when fend_time = '1970-01-01 00:00:00' then
                          0
                         else
                          unix_timestamp(fend_time) - unix_timestamp(fstart_time)
                       end fcosttime,
                       hour(fstart_time) fhour,
                       coalesce(fm_operator,'未知') fm_operator,
                       coalesce(fm_pixel,'未知') fm_pixel,
                       coalesce(fm_network,'未知') fm_network,
                       coalesce(fm_dtype,'未知') fm_dtype
                  from
                    ( select * from stage.game_performance_stg where dt = '%(ld_begin)s'
                        union
                    select * from stage.game_performance_stg_import_tmp where dt = '%(ld_begin)s'
                        ) m
                  left join (select fbpid,
                                   fuid,
                                   fm_operator,
                                   fm_pixel,
                                   fm_network,
                                   fm_dtype
                              from (select fbpid,
                                           fuid,
                                           fm_operator,
                                           fm_pixel,
                                           fm_network,
                                           fm_dtype,
                                           row_number() over(partition by fbpid, fuid order by flogin_at desc) as flag
                                      from stage.user_login_stg a
                                     where a.dt = '%(ld_begin)s') ss
                             where flag = 1) n
                    on m.fbpid = n.fbpid
                   and m.fuid = n.fuid
                  join analysis.bpid_platform_game_ver_map bpm
                    on m.fbpid = bpm.fbpid
                 ) aa
        where fcosttime>=0
          and fcosttime<500
         group by fgamefsk,
                  fplatformfsk,
                  fversionfsk,
                  fterminalfsk,
                  fterminalsysname,
                  fptype,
                  fact_type,
                  fret_code,
                  fret_msg,
                  fhour,
                  fm_operator,
                  fm_pixel,
                  fm_network,
                  fm_dtype


        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res





if __name__ == '__main__':
    a = agg_game_performance()
    a()

