#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class agg_gamecoin_balance(BaseStatModel):

    def create_tab(self):
        hql = """
        create table if not exists dcnew.gamecoin_balance
        (
            fdate            date,
            fgamefsk         bigint,
            fplatformfsk     bigint,
            fhallfsk         bigint,
            fsubgamefsk      bigint,
            fterminaltypefsk bigint,
            fversionfsk      bigint,
            fchannelcode     bigint,
            fbalance         bigint,
            fbybalance       bigint,
            fbalance_act     bigint,
            fbalance_bank    bigint
        )
        partitioned by (dt date);
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res


    def stat(self):
        """ 重要部分，统计内容 """
        alias_dic = {'bpid_tbl_alias':'b.','src_tbl_alias':'a.', 'const_alias':''}

        query = sql_const.query_list(self.stat_date, alias_dic, None)
        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0: return res

        hql = """
        drop table if exists work.gamecoin_bal_tmp_b_%(statdatenum)s;
        create table work.gamecoin_bal_tmp_b_%(statdatenum)s as
                        select /*+ MAPJOIN(b) */ b.fgamefsk,
                               b.fplatformfsk,
                               b.fhallfsk,
                               b.fterminaltypefsk,
                               b.fversionfsk,
                               b.hallmode,
                               a.fgame_id,
                               a.fchannel_code,
                               c.fuid,
                               c.user_gamecoins_num
                          from dim.user_gamecoin_balance c
                          join dim.bpid_map b
                            on c.fbpid = b.fbpid
                          left join dim.user_gamecoin_balance_day a
                            on c.fbpid = a.fbpid
                           and c.fuid = a.fuid
                           and a.dt = "%(statdate)s"
                         where c.dt='%(statdate)s' and c.user_gamecoins_num >= 0
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                       sum(user_gamecoins_num) fbalance,
                       cast (0 as bigint) fbybalance,
                       cast (0 as bigint) fbalance_act,
                       cast (0 as bigint) fbalance_bank
                  from work.gamecoin_bal_tmp_b_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code
        """
        self.sql_template_build(sql=hql)

        hql = """
        drop table if exists work.gamecoin_balance_1_%(statdatenum)s;
        create table work.gamecoin_balance_1_%(statdatenum)s as
        %(sql_template)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """
        drop table if exists work.gamecoin_bal_tmp_b_a_%(statdatenum)s;
        create table work.gamecoin_bal_tmp_b_a_%(statdatenum)s as
                        select /*+ MAPJOIN(b) */ b.fgamefsk,
                               b.fplatformfsk,
                               b.fhallfsk,
                               b.fterminaltypefsk,
                               b.fversionfsk,
                               b.hallmode,
                               a.fgame_id,
                               a.fchannel_code,
                               a.fuid,
                               a.fgamecoins
                          from dim.user_gamecoin_balance_day a
                          join (select distinct fbpid, fuid
                                  from dim.user_act
                                 where dt = "%(statdate)s"
                               ) p
                            on p.fbpid = a.fbpid
                           and p.fuid = a.fuid
                          join dim.bpid_map b
                            on a.fbpid = b.fbpid
                         where a.dt='%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
                select fgamefsk,
                       coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                       coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                       coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                       coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                       coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                       coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                       0 fbalance,
                       0 fbybalance,
                       sum(fgamecoins) fbalance_act,
                       0 fbalance_bank
                  from work.gamecoin_bal_tmp_b_a_%(statdatenum)s gs
                 where hallmode = %(hallmode)s
              group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code
        """
        self.sql_template_build(sql=hql)

        hql = """
        insert into table work.gamecoin_balance_1_%(statdatenum)s
        %(sql_template)s;
        """

        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """
        drop table if exists work.user_bank_2_%(statdatenum)s;
        create table work.user_bank_2_%(statdatenum)s
        as
        select fbpid, fuid,fgame_id,fchannel_code, fbank_gamecoins_num
        from (
            select fbpid, fuid,
                   coalesce(fgame_id,-13658) fgame_id,
                   coalesce(cast (fchannel_code as bigint),-13658) fchannel_code,
                   fbank_gamecoins_num,
                   row_number() over(partition by fbpid, fuid order by flts_at desc, fbank_gamecoins_num desc) rown
              from stage.user_bank_stage
             where dt="%(statdate)s" and fact_type in (0, 1)
               and coalesce(fcurrencies_type,'0') = '0'
             ) t
        where rown = 1
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        for k, v in enumerate(query):
            hql = """
            insert into table work.gamecoin_balance_1_%(statdatenum)s
              select %(select_field_str)s,
                     0 fbalance,
                     0 fbybalance,
                     0 fbalance_act,
                     sum(fbank_gamecoins_num) fbalance_bank
                from work.user_bank_2_%(statdatenum)s a
                join dim.bpid_map b
                  on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
              %(group_by)s
            """
            hql = self.sql_build(hql, v)
            res = self.sql_exe(hql)
            if res != 0:return res


        for k, v in enumerate(query):
            hql = """
            insert into table work.gamecoin_balance_1_%(statdatenum)s
            select   %(select_field_str)s,
                     0 fbalance,
                     sum(user_bycoins_num) fbybalance,
                     0 fbalance_act,
                     0 fbalance_bank
                from dim.user_bycoin_balance c
                left join dim.user_act a
                  on c.fbpid = a.fbpid
                 and c.fuid = a.fuid
                 and a.dt = "%(statdate)s"
                join dim.bpid_map b
                  on c.fbpid = b.fbpid and b.hallmode=%(hallmode)s
               where c.dt='%(statdate)s' and user_bycoins_num >= 0
               %(group_by)s
            """
            hql = self.sql_build(hql, v)
            res = self.sql_exe(hql)
            if res != 0:return res


        hql = """
        insert overwrite table dcnew.gamecoin_balance
        partition(dt="%(statdate)s")
        select "%(statdate)s" fdate,
                 fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                 max(fbalance) fbalance,
                 max(fbybalance) fbybalance,
                 max(fbalance_act) fbalance_act,
                 max(fbalance_bank) fbalance_bank
                from work.gamecoin_balance_1_%(statdatenum)s a
        group by fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        # 统计完清理掉临时表
        hql = """drop table if exists work.gamecoin_bal_tmp_b_%(statdatenum)s;
                 drop table if exists work.gamecoin_bal_tmp_b_a_%(statdatenum)s;
                 drop table if exists work.gamecoin_balance_1_%(statdatenum)s;
                 drop table if exists work.user_bank_2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res


a = agg_gamecoin_balance(sys.argv[1:])
a()
