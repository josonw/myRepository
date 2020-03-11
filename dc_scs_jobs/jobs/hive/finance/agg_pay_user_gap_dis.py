#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


""" 付费用户距离注册时间间隔和上次付费时间间隔的分布
    合并了老后台的两张表analysis.user_pay_days_gap_fct,analysis.user_pay_gap_range_fct,因为老表部分字段已废弃

"""


class agg_pay_user_gap_dis(BaseStatModel):

    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_gap_dis
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fdaysgap                    bigint,
            fregusercnt                 bigint,  --距离注册日期的当天首付用户的人数
            fpayusercnt                 bigint   --距离上次付费的用户人数
        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        extend_group = {'fields':['fdaysgap'],
                        'groups':[[1]]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.pay_user_gap_dis_%(statdatenum)s;
        create table work.pay_user_gap_dis_%(statdatenum)s as
             select b.fgamefsk,
                    b.fplatformfsk,
                    b.fhallfsk,
                    b.fterminaltypefsk,
                    b.fversionfsk,
                    b.hallmode,
                    a.fbpid,
                    a.fuid,
                    a.fgame_id,
                    a.fchannel_code,
                    datediff(a.dt, c.dt) fdaysgap,
                    0 flag
               from dim.user_pay_day a
               join (select fbpid,fuid, max(dt) dt
                       from dim.user_pay_day
                      where dt < '%(statdate)s'
                      group by fbpid,fuid
                               ) c
                 on a.fbpid = c.fbpid
                and a.fuid = c.fuid
               join dim.bpid_map b
                 on a.fbpid = b.fbpid
              where a.dt = '%(statdate)s'
              group by b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                    a.fbpid, a.fuid, a.fgame_id, a.fchannel_code,
                    datediff(a.dt, c.dt)
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
        insert into table work.pay_user_gap_dis_%(statdatenum)s
             select b.fgamefsk,
                    b.fplatformfsk,
                    b.fhallfsk,
                    b.fterminaltypefsk,
                    b.fversionfsk,
                    b.hallmode,
                    a.fbpid,
                    a.fuid,
                    a.fgame_id,
                    a.fchannel_code,
                    min(datediff(a.dt, coalesce(cast (c.dt as date), cast('2001-01-01' as date)))) fdaysgap,
                    1 flag
               from dim.user_pay a
               left join (select fbpid,fuid,dt
                       from dim.reg_user_main_additional
                      where dt <= '%(statdate)s'
                               ) c
                 on c.fbpid = a.fbpid
                and c.fuid = a.fuid
               join dim.bpid_map b
                 on a.fbpid = b.fbpid
              where a.dt = '%(statdate)s'
              group by b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk,b.hallmode,
                    a.fbpid, a.fuid, a.fgame_id, a.fchannel_code
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
         select '%(statdate)s' fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                fdaysgap,
                count(DISTINCT case when flag=1 then a.fuid else null end) fregusercnt,
                count(DISTINCT case when flag=0 then a.fuid else null end) fpayusercnt
           from work.pay_user_gap_dis_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fdaysgap
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.pay_user_gap_dis
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.pay_user_gap_dis_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_pay_user_gap_dis(sys.argv[1:])
a()
