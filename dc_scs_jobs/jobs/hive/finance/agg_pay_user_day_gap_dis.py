#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


class agg_pay_user_day_gap_dis(BaseStatModel):
    """ 多个付费周期内付费用户的天数分布"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_day_gap_dis
        (
            fdate               date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            ftype               int,
            f1cnt               bigint,
            f2cnt               bigint,
            f3cnt               bigint,
            f4cnt               bigint,
            f5cnt               bigint,
            f6cnt               bigint,
            f7cnt               bigint,
            f8cnt               bigint,
            f9cnt               bigint,
            f10cnt              bigint,
            f11cnt              bigint,
            f12cnt              bigint,
            f13cnt              bigint,
            f14cnt              bigint,
            f15cnt              bigint,
            f16cnt              bigint,
            f17cnt              bigint,
            f18cnt              bigint,
            f19cnt              bigint,
            f20cnt              bigint,
            f21cnt              bigint,
            f22cnt              bigint,
            f23cnt              bigint,
            f24cnt              bigint,
            f25cnt              bigint,
            f26cnt              bigint,
            f27cnt              bigint,
            f28cnt              bigint,
            f29cnt              bigint,
            f30cnt              bigint
        )
        partitioned by (dt date);
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.pay_user_day_gap_dis_%(statdatenum)s;
        create table work.pay_user_day_gap_dis_%(statdatenum)s as
        select b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                a.fbpid, a.fuid, a.fgame_id, a.fchannel_code,
                count(distinct case when a.dt>='%(ld_7day_ago)s' and a.dt<'%(ld_end)s' then a.dt else null end) pay_7cnt,
                count(distinct case when a.dt>='%(ld_14day_ago)s' and a.dt<'%(ld_end)s' then a.dt else null end) pay_14cnt,
                count(distinct case when a.dt>='%(ld_30day_ago)s' and a.dt<'%(ld_end)s' then a.dt else null end) pay_30cnt
            from dim.user_pay_day a
            join dim.bpid_map b
              on a.fbpid = b.fbpid
           where a.dt >= date_add('%(statdate)s', -30)  and  a.dt < '%(ld_end)s'
           group by b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                    a.fbpid, a.fuid, a.fgame_id, a.fchannel_code
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        select  '%(statdate)s' fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                7 ftype,
            count(distinct case when a.pay_7cnt=1 then a.fuid end) f1cnt, count(distinct case when a.pay_7cnt=2 then a.fuid end) f2cnt,
            count(distinct case when a.pay_7cnt=3 then a.fuid end) f3cnt, count(distinct case when a.pay_7cnt=4 then a.fuid end) f4cnt,
            count(distinct case when a.pay_7cnt=5 then a.fuid end) f5cnt, count(distinct case when a.pay_7cnt=6 then a.fuid end) f6cnt,
            count(distinct case when a.pay_7cnt=7 then a.fuid end) f7cnt,
            0 f8cnt,
            0 f9cnt,
            0 f10cnt,
            0 f11cnt,
            0 f12cnt,
            0 f13cnt,
            0 f14cnt,
            0 f15cnt,
            0 f16cnt,
            0 f17cnt,
            0 f18cnt,
            0 f19cnt,
            0 f20cnt,
            0 f21cnt,
            0 f22cnt,
            0 f23cnt,
            0 f24cnt,
            0 f25cnt,
            0 f26cnt,
            0 f27cnt,
            0 f28cnt,
            0 f29cnt,
            0 f30cnt
         from work.pay_user_day_gap_dis_%(statdatenum)s a
        where hallmode = %(hallmode)s
        group by fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
                 fgame_id, fchannel_code
        """
        self.sql_template_build(sql=hql)

        hql = """
        insert overwrite table dcnew.pay_user_day_gap_dis
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        select  '%(statdate)s' fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                14 ftype,
            count(distinct case when a.pay_14cnt=1 then a.fuid end) f1cnt, count(distinct case when a.pay_14cnt=2 then a.fuid end) f2cnt,
            count(distinct case when a.pay_14cnt=3 then a.fuid end) f3cnt, count(distinct case when a.pay_14cnt=4 then a.fuid end) f4cnt,
            count(distinct case when a.pay_14cnt=5 then a.fuid end) f5cnt, count(distinct case when a.pay_14cnt=6 then a.fuid end) f6cnt,
            count(distinct case when a.pay_14cnt=7 then a.fuid end) f7cnt, count(distinct case when a.pay_14cnt=8 then a.fuid end) f8cnt,
            count(distinct case when a.pay_14cnt=9 then a.fuid end) f9cnt, count(distinct case when a.pay_14cnt=10 then a.fuid end) f10cnt,
            count(distinct case when a.pay_14cnt=11 then a.fuid end) f11cnt, count(distinct case when a.pay_14cnt=12 then a.fuid end) f12cnt,
            count(distinct case when a.pay_14cnt=13 then a.fuid end) f13cnt, count(distinct case when a.pay_14cnt=14 then a.fuid end) f14cnt,
            0 f15cnt,
            0 f16cnt,
            0 f17cnt,
            0 f18cnt,
            0 f19cnt,
            0 f20cnt,
            0 f21cnt,
            0 f22cnt,
            0 f23cnt,
            0 f24cnt,
            0 f25cnt,
            0 f26cnt,
            0 f27cnt,
            0 f28cnt,
            0 f29cnt,
            0 f30cnt
         from work.pay_user_day_gap_dis_%(statdatenum)s a
        where hallmode = %(hallmode)s
        group by fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
                 fgame_id, fchannel_code
        """
        self.sql_template_build(sql=hql)

        hql = """
        insert into table dcnew.pay_user_day_gap_dis
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        select  '%(statdate)s' fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                30 ftype,
            count(distinct case when a.pay_30cnt=1 then a.fuid end) f1cnt, count(distinct case when a.pay_30cnt=2 then a.fuid end) f2cnt,
            count(distinct case when a.pay_30cnt=3 then a.fuid end) f3cnt, count(distinct case when a.pay_30cnt=4 then a.fuid end) f4cnt,
            count(distinct case when a.pay_30cnt=5 then a.fuid end) f5cnt, count(distinct case when a.pay_30cnt=6 then a.fuid end) f6cnt,
            count(distinct case when a.pay_30cnt=7 then a.fuid end) f7cnt, count(distinct case when a.pay_30cnt=8 then a.fuid end) f8cnt,
            count(distinct case when a.pay_30cnt=9 then a.fuid end) f9cnt, count(distinct case when a.pay_30cnt=10 then a.fuid end) f10cnt,
            count(distinct case when a.pay_30cnt=11 then a.fuid end) f11cnt, count(distinct case when a.pay_30cnt=12 then a.fuid end) f12cnt,
            count(distinct case when a.pay_30cnt=13 then a.fuid end) f13cnt, count(distinct case when a.pay_30cnt=14 then a.fuid end) f14cnt,
            count(distinct case when a.pay_30cnt=15 then a.fuid end) f15cnt, count(distinct case when a.pay_30cnt=16 then a.fuid end) f16cnt,
            count(distinct case when a.pay_30cnt=17 then a.fuid end) f17cnt, count(distinct case when a.pay_30cnt=18 then a.fuid end) f18cnt,
            count(distinct case when a.pay_30cnt=19 then a.fuid end) f19cnt, count(distinct case when a.pay_30cnt=20 then a.fuid end) f20cnt,
            count(distinct case when a.pay_30cnt=21 then a.fuid end) f21cnt, count(distinct case when a.pay_30cnt=22 then a.fuid end) f22cnt,
            count(distinct case when a.pay_30cnt=23 then a.fuid end) f23cnt, count(distinct case when a.pay_30cnt=24 then a.fuid end) f24cnt,
            count(distinct case when a.pay_30cnt=25 then a.fuid end) f25cnt, count(distinct case when a.pay_30cnt=26 then a.fuid end) f26cnt,
            count(distinct case when a.pay_30cnt=27 then a.fuid end) f27cnt, count(distinct case when a.pay_30cnt=28 then a.fuid end) f28cnt,
            count(distinct case when a.pay_30cnt=29 then a.fuid end) f29cnt, count(distinct case when a.pay_30cnt=30 then a.fuid end) f30cnt
         from work.pay_user_day_gap_dis_%(statdatenum)s a
        where hallmode = %(hallmode)s
        group by fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
                 fgame_id, fchannel_code
        """
        self.sql_template_build(sql=hql)

        hql = """
        insert into table dcnew.pay_user_day_gap_dis
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.pay_user_day_gap_dis_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_pay_user_day_gap_dis(sys.argv[1:])
a()
