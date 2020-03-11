#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat


class agg_currencies_qujian(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
              create table if not exists analysis.user_currencies_scatter_fct(
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fversionfsk bigint,
                fterminalfsk bigint,
                fc_type varchar(50),
                fcnt bigint,
                flft bigint,
                frgt bigint)
              partitioned by (dt date)
              location '/dw/analysis/user_currencies_scatter_fct'
        """
        res = self.hq.exe_sql(hql)

        if res != 0:
            return res

        return res


        hql = """
              create table if not exists analysis.currencies_name_dim(
                fsk      bigint,
                fgamefsk bigint,
                fc_type  varchar(50),
                fdis_name varchar(50)
              )
              location '/dw/analysis/currencies_name_dim'
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

        hql = """ -- 算出用户货币当日初始携带量
              use stage;
              drop table if exists stage.user_currencies_first_tmp_%(num_date)s;
              create table stage.user_currencies_first_tmp_%(num_date)s as
              select b.fgamefsk fgamefsk,
                     b.fplatformfsk fplatformfsk,
                     b.fversionfsk fversionfsk,
                     b.fterminalfsk fterminalfsk,
                     t.fuid fuid,
                     t.fc_type fc_type,
                     t.fc_num fc_num
                from analysis.bpid_platform_game_ver_map b
                join(
                  select fbpid, fuid, fcurrencies_type fc_type, fcurrencies_num fc_num
                  from (
                      select fbpid, fuid, fcurrencies_type, fcurrencies_num,
                          row_number() over(partition by fbpid, fuid, fcurrencies_type order by flts_at,
                           fseq_no) rown
                      from stage.pb_currencies_stream_stg
                      where dt = "%(ld_begin)s"
                  ) ss
                  where ss.rown = 1
                ) t
                  on t.fbpid = b.fbpid
               ;
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
              insert overwrite table analysis.user_currencies_scatter_fct
              partition(dt="%(ld_begin)s")
              select "%(ld_begin)s" fdate,
                     a.fgamefsk fgamefsk,
                     a.fplatformfsk fplatformfsk,
                     a.fversionfsk fversionfsk,
                     a.fterminalfsk fterminalfsk,
                     a.fc_type fc_type,
                     count(a.fuid) fcnt,
                     b.flft flft,
                     b.frgt frgt
                from stage.user_currencies_first_tmp_%(num_date)s a
                join stage.jw_qujian b
               where a.fc_num >= b.flft
                 and a.fc_num < b.frgt
               group by a.fgamefsk,
                        a.fplatformfsk,
                        a.fversionfsk,
                        a.fterminalfsk,
                        a.fc_type,
                        b.flft,
                        b.frgt
              """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
          add file hdfs://192.168.0.92:8020/dw/streaming/pg_exchange.py;

          insert into table analysis.currencies_name_dim
          select transform('analysis.currencies_name_dim', 'fgamefsk,fc_type,fdis_name', c.fgamefsk, c.fc_type, c.fdis_name)
            using 'pg_exchange.py'
            as (fsk bigint, fgamefsk bigint, fc_type string, fdis_name bigint)
          from (
              select a.fgamefsk fgamefsk, a.fc_type fc_type, a.fc_type fdis_name
              from (
                select distinct fgamefsk, fc_type
                from analysis.user_currencies_scatter_fct
                where dt="%(ld_begin)s"
              ) a
              left outer join analysis.currencies_name_dim gd
                on gd.fgamefsk = a.fgamefsk and gd.fc_type=a.fc_type
              where gd.fc_type is null
          ) as c;
        """ % args_dic

        hql += """
        drop table if exists user_currencies_first_tmp_%(num_date)s;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


if __name__ == '__main__':
    a = agg_currencies_qujian()
    a()
