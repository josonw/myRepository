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



class agg_pay_user_actret_payret_lastmonth(BaseStatModel):
    """ 上月（自然月）付费用户，在本月的付费留存，活跃留存"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_actret_payret_lastmonth
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            flastmonthppr               bigint,
            flastmonthpar               bigint
        )partitioned by (dt date);
        """
        result = self.sql_exe(hql)
        if result != 0:return result


    def stat(self):
        """ 重要部分，统计内容 """
        # self.hq.debug = 1

        hql_list = []

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        alias_dic = {'bpid_tbl_alias':'a.','src_tbl_alias':'a.', 'const_alias':''}
        query = sql_const.query_list(self.stat_date, alias_dic, None)

        for k, v in enumerate(query):
            hql = """
            select "%(statdate)s" fdate,
                %(select_field_str)s,
                count(distinct fuid) flastmonthppr,
                0     flastmonthpar
            from
            (
                select a.fgamefsk, a.fplatformfsk, a.fhallfsk, a.fgame_id, a.fterminaltypefsk, a.fversionfsk,
                       a.fchannel_code, a.fuid, a.fplatform_uid, max(flag1), max(flag2)
                from
                (
                    select %(select_subquery)s, a.fuid, fplatform_uid, 1 flag1, 0 flag2
                    from dim.user_pay_day a
                    join dim.bpid_map b
                      on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
                    where a.dt >= '%(ld_1month_ago_begin)s' and a.dt < '%(ld_month_begin)s'

                    union all

                    select %(select_subquery)s, a.fuid, fplatform_uid, 0 flag1, 1 flag2
                    from dim.user_pay_day a
                    join dim.bpid_map b
                      on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
                    where a.dt >= '%(ld_month_begin)s' and a.dt < '%(ld_1month_after_begin)s'
                ) a
                group by a.fgamefsk, a.fplatformfsk, a.fhallfsk, a.fgame_id, a.fterminaltypefsk, a.fversionfsk,
                         a.fchannel_code, a.fuid, a.fplatform_uid
                having max(flag1) = 1 and max(flag2) = 1
            ) a
            %(group_by)s
            """
            self.sql_args['sql_'+ str(k)] = self.sql_build(hql, v)


        hql = """
        drop table if exists work.pay_user_actret_payret_lastmonth_%(statdatenum)s;
        create table work.pay_user_actret_payret_lastmonth_%(statdatenum)s as
        %(sql_0)s;
        insert into table work.pay_user_actret_payret_lastmonth_%(statdatenum)s
        %(sql_1)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        for k, v in enumerate(query):
            hql = """
            insert into table work.pay_user_actret_payret_lastmonth_%(statdatenum)s
            select '%(statdate)s' fdate,
                %(select_field_str)s,
                0     flastmonthppr,
                count(distinct fuid) flastmonthpar
            from
            (
                select a.fgamefsk, a.fplatformfsk, a.fhallfsk, max(a.fgame_id) fgame_id, a.fterminaltypefsk, a.fversionfsk,
                       max(a.fchannel_code) fchannel_code, fuid, max(flag1), max(flag2)
                from
                (
                    select %(select_subquery)s, a.fuid, 1 flag1, 0 flag2
                    from dim.user_pay_day a
                    join dim.bpid_map b
                      on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
                    join dim.user_pay c
                      on a.fbpid = c.fbpid and a.fuid = c.fuid
                    where a.dt >= '%(ld_1month_ago_begin)s' and a.dt < '%(ld_month_begin)s'

                    union all

                    select %(select_subquery)s, a.fuid, 0 flag1, 1 flag2
                    from dim.user_act a
                    join dim.bpid_map b
                      on a.fbpid = b.fbpid and b.hallmode=%(hallmode)s
                    where a.dt >= '%(ld_month_begin)s' and a.dt < '%(ld_1month_after_begin)s'
                ) a
                group by a.fgamefsk, a.fplatformfsk, a.fhallfsk, a.fterminaltypefsk, a.fversionfsk,
                         fuid
                having max(flag1) = 1 and max(flag2) = 1
            ) a
            %(group_by)s
            """
            hql = self.sql_build(hql, v)
            res = self.sql_exe(hql)
            if res != 0:return res

        hql = """
        insert overwrite table dcnew.pay_user_actret_payret_lastmonth
        partition( dt="%(statdate)s" )
        select fdate,
            fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
            max(flastmonthppr) flastmonthppr,
            max(flastmonthpar) flastmonthpar
        from work.pay_user_actret_payret_lastmonth_%(statdatenum)s
        group by fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        res = self.sql_exe("""drop table if exists work.pay_user_actret_payret_lastmonth_%(statdatenum)s;""")
        if res != 0:return res

        return res


# 生成统计实例
a = agg_pay_user_actret_payret_lastmonth(sys.argv[1:])
a()
