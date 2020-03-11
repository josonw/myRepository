# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_user_property_dis(BaseStatModel):
    def create_tab(self):

        hql = """--
        create table if not exists dcnew.user_property_dis
            (
            fdate date,
            fgamefsk bigint,
            fplatformfsk bigint,
            fhallfsk bigint,
            fsubgamefsk bigint,
            fterminaltypefsk bigint,
            fversionfsk bigint,
            fchannelcode bigint,
            fdimension varchar(32),
            feq0 bigint,
            flq5k bigint,
            fm5Klq10k bigint,
            fm10Klq50k bigint,
            fm50Klq100k bigint,
            fm100Klq500k bigint,
            fm500Klq1m bigint,
            fm1mlq5m bigint,
            fm5mlq10m bigint,
            fm10mlq50m bigint,
            fm50mlq100m bigint,
            fm100mlq1b bigint,
            fm1b bigint
            )
            partitioned by (dt date)
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        #调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        extend_group = {'fields':[],
                        'groups':[] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res
        #res = self.sql_exe("set hive.execution.engine=spark")

        hql = """--注册用户
        drop table if exists work.new_property_tmp_%(statdatenum)s;
        create table work.new_property_tmp_%(statdatenum)s as
              select c.fgamefsk,
                     c.fplatformfsk,
                     c.fhallfsk,
                     c.fterminaltypefsk,
                     c.fversionfsk,
                     c.hallmode,
                     a.fgame_id,
                     a.fchannel_code,
                     a.fgamecoins,
                     a.fuid
                from dim.user_gamecoin_balance_day a
                join dim.reg_user_main_additional b
                  on a.fbpid = b.fbpid
                 and a.fuid = b.fuid
                 and b.dt = "%(statdate)s"
                join dim.bpid_map c
                  on a.fbpid=c.fbpid
               where a.dt = "%(statdate)s"

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       'register' fdimension,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) = 0 then fuid else null end),0) feq0,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 1 and coalesce(fgamecoins,0) < 5000 then fuid else null end),0) flq5k,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 5000 and coalesce(fgamecoins,0) < 10000 then fuid else null end),0) fm5Klq10k,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 10000 and coalesce(fgamecoins,0) < 50000 then fuid else null end),0) fm10Klq50k,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 50000 and coalesce(fgamecoins,0) < 100000 then fuid else null end),0) fm50Klq100k,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 100000 and coalesce(fgamecoins,0) < 500000 then fuid else null end),0) fm100Klq500k,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 500000 and coalesce(fgamecoins,0) < 1000000 then fuid else null end),0) fm500Klq1m,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 1000000 and coalesce(fgamecoins,0) < 5000000 then fuid else null end),0) fm1mlq5m,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 5000000 and coalesce(fgamecoins,0) < 10000000 then fuid else null end),0) fm5mlq10m,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 10000000 and coalesce(fgamecoins,0) < 50000000 then fuid else null end),0) fm10mlq50m,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 50000000 and coalesce(fgamecoins,0) < 100000000 then fuid else null end),0) fm50mlq100m,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 100000000 and coalesce(fgamecoins,0) < 1000000000 then fuid else null end),0) fm100mlq1b,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 1000000000 then fuid else null end),0) fm1b
                  from work.new_property_tmp_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code
        """
        self.sql_template_build(sql=hql)

        hql = """
        insert overwrite table dcnew.user_property_dis partition(dt="%(statdate)s")
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """--活跃用户——当日有金流变化
        drop table if exists work.act_property_tmp_%(statdatenum)s;
        create table work.act_property_tmp_%(statdatenum)s as
              select c.fgamefsk,
                     c.fplatformfsk,
                     c.fhallfsk,
                     c.fterminaltypefsk,
                     c.fversionfsk,
                     c.hallmode,
                     a.fgame_id,
                     a.fchannel_code,
                     a.fgamecoins,
                     a.fuid
                from dim.user_gamecoin_balance_day a
                join dim.user_act b
                  on a.fbpid = b.fbpid
                 and a.fuid = b.fuid
                 and b.dt = "%(statdate)s"
                join dim.bpid_map c
                  on a.fbpid=c.fbpid
               where a.dt = "%(statdate)s"

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """--活跃用户——当日无金流变化，取90天内最近一次金流变化
        insert into table work.act_property_tmp_%(statdatenum)s
                select c.fgamefsk,
                       c.fplatformfsk,
                       c.fhallfsk,
                       c.fterminaltypefsk,
                       c.fversionfsk,
                       c.hallmode,
                       a.fgame_id,
                       a.fchannel_code,
                       a.fgamecoins,
                       a.fuid
                  from (select a.fbpid,
                               p.fgame_id,
                               p.fchannel_code,
                               a.fuid,
                               p.fgamecoins,
                               row_number() over(partition by a.fbpid, a.fuid order by p.fdate desc, p.fgamecoins desc) rown
                          from (
                                -- 当日活跃，但是没有游戏币变化记录
                                select a.fbpid, a.fuid
                                  from dim.user_act a
                                  left join dim.user_gamecoin_balance_day p
                                    on a.fbpid = p.fbpid and a.fuid = p.fuid
                                   and p.dt = "%(statdate)s"
                                 where a.dt = "%(statdate)s"
                                   and p.fbpid is null
                               ) a
                          left join dim.user_gamecoin_balance_day p
                            on a.fbpid = p.fbpid
                           and a.fuid = p.fuid
                         where p.dt >= '%(ld_90day_ago)s' and p.dt < '%(statdate)s'
                       ) a
                  join dim.bpid_map c
                    on a.fbpid=c.fbpid
                 where a.rown = 1;

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """
                select "%(statdate)s" fdate,
                       fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                       'active' fdimension,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) = 0 then fuid else null end),0) feq0,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 1 and coalesce(fgamecoins,0) < 5000 then fuid else null end),0) flq5k,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 5000 and coalesce(fgamecoins,0) < 10000 then fuid else null end),0) fm5Klq10k,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 10000 and coalesce(fgamecoins,0) < 50000 then fuid else null end),0) fm10Klq50k,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 50000 and coalesce(fgamecoins,0) < 100000 then fuid else null end),0) fm50Klq100k,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 100000 and coalesce(fgamecoins,0) < 500000 then fuid else null end),0) fm100Klq500k,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 500000 and coalesce(fgamecoins,0) < 1000000 then fuid else null end),0) fm500Klq1m,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 1000000 and coalesce(fgamecoins,0) < 5000000 then fuid else null end),0) fm1mlq5m,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 5000000 and coalesce(fgamecoins,0) < 10000000 then fuid else null end),0) fm5mlq10m,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 10000000 and coalesce(fgamecoins,0) < 50000000 then fuid else null end),0) fm10mlq50m,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 50000000 and coalesce(fgamecoins,0) < 100000000 then fuid else null end),0) fm50mlq100m,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 100000000 and coalesce(fgamecoins,0) < 1000000000 then fuid else null end),0) fm100mlq1b,
                       coalesce(count(distinct case when coalesce(fgamecoins,0) >= 1000000000 then fuid else null end),0) fm1b
                  from work.act_property_tmp_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code
        """
        self.sql_template_build(sql=hql)

        hql = """
        insert into table dcnew.user_property_dis partition(dt="%(statdate)s")
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.new_property_tmp_%(statdatenum)s;
                 drop table if exists work.act_property_tmp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


#生成统计实例
a = agg_user_property_dis(sys.argv[1:])
a()
