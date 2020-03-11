#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat

# 这个脚本从agg_gamecoin_day_balance.py分离出来的
# 目的是把中间表和统计内容分开

class agg_gamecoin_day_balance_fct(BaseStat):

    def create_tab(self):
        #建表的时候正式执行
        self.hq.debug = 0

        hql = """
        create external table if not exists analysis.gamecoin_balance_fct
        (
            fdate          date,
            fgamefsk       bigint,
            fplatformfsk   bigint,
            fversionfsk    bigint,
            fterminalfsk   bigint,
            fbalance       bigint,
            fbybalance     bigint,
            fbalance_act   bigint,
            fbalance_bank  bigint
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
            "num_begin": self.stat_date.replace('-', '')
        }

        res=0

        hql = """
        drop table if exists stage.agg_gamecoin_day_balance_fct_1_%(num_begin)s;

        create table stage.agg_gamecoin_day_balance_fct_1_%(num_begin)s as
        select fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 sum(user_gamecoins_num) fbalance
            from stage.user_gamecoins_balance_mid
           where dt='%(ld_begin)s' and user_gamecoins_num >= 0
        group by fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        set hive.mapjoin.smalltable.filesize=10000000;

        drop table if exists stage.agg_gamecoin_day_balance_fct_2_%(num_begin)s;

        create table stage.agg_gamecoin_day_balance_fct_2_%(num_begin)s as
        select b.fgamefsk,
                 b.fplatformfsk,
                 b.fversionfsk,
                 b.fterminalfsk,
                 sum(user_gamecoins_num) fbalance_act
            from stage.pb_gamecoins_stream_mid p
            join stage.active_user_mid a
              on a.dt = "%(ld_begin)s"
             and p.fbpid = a.fbpid
             and p.fuid = a.fuid
            join analysis.bpid_platform_game_ver_map b
              on p.fbpid = b.fbpid
           where p.dt = "%(ld_begin)s"
        group by b.fgamefsk,
                 b.fplatformfsk,
                 b.fversionfsk,
                 b.fterminalfsk;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists stage.agg_gamecoin_day_balance_fct_3_%(num_begin)s;

        create table stage.agg_gamecoin_day_balance_fct_3_%(num_begin)s as
        select b.fgamefsk,
                 b.fplatformfsk,
                 b.fversionfsk,
                 b.fterminalfsk,
                 sum(fbank_gamecoins_num) fbalance_bank
            from (
                  select fbpid, fuid, fbank_gamecoins_num,
                         row_number() over(partition by fbpid, fuid order by flts_at desc, fbank_gamecoins_num desc) rown
                    from stage.user_bank_stage
                   where dt="%(ld_begin)s"
                     and fact_type in (0, 1)
                     and coalesce(fcurrencies_type,'0') = '0'
                 ) t
            join analysis.bpid_platform_game_ver_map b
              on t.fbpid = b.fbpid
           where t.rown = 1
        group by b.fgamefsk,
                 b.fplatformfsk,
                 b.fversionfsk,
                 b.fterminalfsk;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists stage.agg_bycoin_day_balance_fct_4_%(num_begin)s;

        create table stage.agg_bycoin_day_balance_fct_4_%(num_begin)s as
        select fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk,
                 sum(user_bycoins_num) fbybalance
            from stage.user_bycoins_balance_mid
           where dt='%(ld_begin)s' and user_bycoins_num >= 0
        group by fgamefsk,
                 fplatformfsk,
                 fversionfsk,
                 fterminalfsk;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        hql = """
        insert overwrite table analysis.gamecoin_balance_fct partition(dt="%(ld_begin)s")
        select "%(ld_begin)s" fdate,
                 a.fgamefsk,
                 a.fplatformfsk,
                 a.fversionfsk,
                 a.fterminalfsk,
                 nvl(a.fbalance, 0) fbalance,
                 nvl(d.fbybalance, 0) fbybalance,
                 nvl(b.fbalance_act, 0) fbalance_act,
                 nvl(c.fbalance_bank, 0) fbalance_bank
            from stage.agg_gamecoin_day_balance_fct_1_%(num_begin)s a
     left outer join stage.agg_gamecoin_day_balance_fct_2_%(num_begin)s b
                  on a.fgamefsk = b.fgamefsk
                 and a.fplatformfsk = b.fplatformfsk
                 and a.fversionfsk = b.fversionfsk
                 and a.fterminalfsk = b.fterminalfsk
     left outer join stage.agg_gamecoin_day_balance_fct_3_%(num_begin)s c
                  on a.fgamefsk = c.fgamefsk
                 and a.fplatformfsk = c.fplatformfsk
                 and a.fversionfsk = c.fversionfsk
                 and a.fterminalfsk = c.fterminalfsk
    left outer join stage.agg_bycoin_day_balance_fct_4_%(num_begin)s d
                  on a.fgamefsk = d.fgamefsk
                 and a.fplatformfsk = d.fplatformfsk
                 and a.fversionfsk = d.fversionfsk
                 and a.fterminalfsk = d.fterminalfsk

        """ % args_dic

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """
        drop table if exists stage.agg_gamecoin_day_balance_fct_1_%(num_begin)s;
        drop table if exists stage.agg_bycoin_day_balance_fct_4_%(num_begin)s;
        drop table if exists stage.agg_gamecoin_day_balance_fct_2_%(num_begin)s;
        drop table if exists stage.agg_gamecoin_day_balance_fct_3_%(num_begin)s;
        """ % args_dic
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res


if __name__ == '__main__':
    a = agg_gamecoin_day_balance_fct()
    a()
