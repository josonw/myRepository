#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


class agg_pay_user_age_dis(BaseStatModel):
    """ 新后台支付通道监控"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.pay_user_age_dis
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
            fcoins_num                 decimal(20,2),
            fpay_unum                 bigint,
            fpay_cnt                 bigint
        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """
        extend_group = {'fields':['fdaysgap'],
                        'groups':[[1]]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.pay_user_age_dis%(statdatenum)s;
        create table work.pay_user_age_dis%(statdatenum)s as
             select distinct b.fgamefsk,
                    b.fplatformfsk,
                    b.fhallfsk,
                    b.fterminaltypefsk,
                    b.fversionfsk,
                    b.hallmode,
                    a.fbpid,
                    a.fuid,
                    a.fgame_id,
                    a.fchannel_code,
                    a.ftotal_usd_amt,
                    a.fpay_cnt,
                    datediff('%(statdate)s', c.dt) fdaysgap
               from dim.user_pay_day a
               join (select fbpid,fplatform_uid,dt,fuid
                       from dim.reg_user_main_additional
                      where dt <= '%(statdate)s'
                               ) c
                 on c.fbpid = a.fbpid
                and c.fuid = a.fuid
               join dim.bpid_map b
                 on a.fbpid = b.fbpid
              where a.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

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
                sum(ftotal_usd_amt) fcoins_num,
                count(DISTINCT a.fuid ) fpay_unum,
                sum(fpay_cnt) fpay_cnt
           from work.pay_user_age_dis%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,fdaysgap
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        insert overwrite table dcnew.pay_user_age_dis
        partition (dt = "%(statdate)s")
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        drop table if exists work.pay_user_age_dis%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 生成统计实例
a = agg_pay_user_age_dis(sys.argv[1:])
a()
