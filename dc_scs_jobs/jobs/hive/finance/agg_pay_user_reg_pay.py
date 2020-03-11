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


class agg_pay_user_reg_pay(BaseStatModel):
    """ 新增用户的付费留存，首付用户的留存 """
    def create_tab(self):
        hql = """
        -- 特殊同步要求, 90天  --没计算出来
        create table if not exists dcnew.pay_user_reg_pay
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fpaydate                    date,
            fpayusernum                 bigint,
            fincome                     decimal(38,2),
            ffirstpayusernum            bigint,
            ffirstincome                decimal(38,2)
        )
        partitioned by (dt date);

        """
        result = self.sql_exe(hql)
        if result != 0:
            return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        # self.hq.debug = 1
        # 特殊同步要求, 90天
        extend_group = {'fields':['fdate'],
                        'groups':[[1] ] }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.pay_user_reg_pay_temp_%(statdatenum)s;
        create table work.pay_user_reg_pay_temp_%(statdatenum)s as
        select
            d.dt fdate,
            b.fgamefsk,
            b.fplatformfsk,
            b.fhallfsk,
            b.fterminaltypefsk,
            b.fversionfsk,
            b.hallmode,
            a.fgame_id,
            a.fchannel_code,
            a.fuid payuid,
            case when c.dt= '%(ld_begin)s' then c.fuid end fpayuid,
            sum(a.ftotal_usd_amt) dip
        from dim.user_pay_day a
        left join dim.user_pay c
          on a.fuid = c.fuid
         and a.fbpid = c.fbpid
        join dim.reg_user_main_additional d
          on c.fbpid = d.fbpid
         and c.fuid = d.fuid
         and d.dt >= '%(ld_90day_ago)s'
         and d.dt <  '%(ld_end)s'
        join dim.bpid_map b
        on a.fbpid = b.fbpid
        where a.dt = '%(ld_begin)s'
        group by d.dt,
            c.dt,
            b.fgamefsk,
            b.fplatformfsk,
            b.fhallfsk,
            b.fterminaltypefsk,
            b.fversionfsk,
            b.hallmode,
            a.fgame_id,
            a.fchannel_code,
            a.fuid,
            c.fuid
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        hql = """
         select fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fgame_id,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannel_code,
                '%(ld_begin)s' fpaydate,
                count(distinct payuid) fpayusernum,
                sum(dip) fincome,
                count(distinct fpayuid) ffirstpayusernum,
                sum(case when fpayuid is not null then dip end) ffirstincome
           from work.pay_user_reg_pay_temp_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk, fchannel_code,fdate
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.pay_user_reg_pay_temp2_%(statdatenum)s;
        create table work.pay_user_reg_pay_temp2_%(statdatenum)s as
        %(sql_template)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        insert overwrite table dcnew.pay_user_reg_pay partition(dt)
        select
        cast (fdate as date),
        fgamefsk,
        fplatformfsk,
        fhallfsk,
        fgame_id,
        fterminaltypefsk,
        fversionfsk,
        fchannel_code,
        cast (fpaydate as date),
        fpayusernum,
        fincome,
        ffirstpayusernum,
        ffirstincome,
        cast (fdate as date) dt
        from work.pay_user_reg_pay_temp2_%(statdatenum)s

        union all

        select fdate,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
                fpaydate,
                fpayusernum,
                fincome,
                ffirstpayusernum,
                ffirstincome,
                fdate dt
         from dcnew.pay_user_reg_pay
         where dt >= '%(ld_90day_ago)s'
           and dt <  '%(ld_end)s'
           and fpaydate != '%(ld_begin)s'
        """
        res = self.sql_exe(hql)
        if res != 0:return res



        hql = """
        insert overwrite table dcnew.pay_user_reg_pay partition(dt='3000-01-01')
        select fdate,
               fgamefsk,
               fplatformfsk,
               fhallfsk,
               fsubgamefsk,
               fterminaltypefsk,
               fversionfsk,
               fchannelcode,
               fpaydate,
               fpayusernum,
               fincome,
               ffirstpayusernum,
               ffirstincome
        from dcnew.pay_user_reg_pay
        where dt >= '%(ld_90day_ago)s'
        and dt < '%(ld_end)s'
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        drop table if exists work.pay_user_reg_pay_temp2_%(statdatenum)s;
        drop table if exists work.pay_user_reg_pay_temp_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res

#生成统计实例
a = agg_pay_user_reg_pay(sys.argv[1:])
a()
