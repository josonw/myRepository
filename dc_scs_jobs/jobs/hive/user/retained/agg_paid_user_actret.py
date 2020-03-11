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



class agg_paid_user_actret(BaseStatModel):
    def create_tab(self):
        hql = """create table if not exists dcnew.paid_user_actret
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fhallfsk bigint,
                fsubgamefsk bigint,
                fterminaltypefsk bigint,
                fversionfsk bigint,
                fchannelcode bigint,
                f1daycnt bigint,
                f2daycnt bigint,
                f3daycnt bigint,
                f4daycnt bigint,
                f5daycnt bigint,
                f6daycnt bigint,
                f7daycnt bigint,
                f14daycnt bigint,
                f30daycnt bigint
                )
                partitioned by (dt date)"""
        res = self.sql_exe(hql)
        if res != 0:return res



    def stat(self):
        extend_group = {
                     'fields':[ 'dt','retday'],
                     'groups':[ [1,1]]
                     }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.paid_user_actret_temp1_%(statdatenum)s;
        create table work.paid_user_actret_temp1_%(statdatenum)s
        as
            select
                bm.fgamefsk, bm.fplatformfsk, bm.fhallfsk, bm.fterminaltypefsk, bm.fversionfsk, bm.hallmode,
                datediff('%(statdate)s', pay.dt) retday,
                pay.dt, pay.fgame_id, pay.fchannel_code, pay.fuid
            from dim.user_act_paid  pay
            join dim.user_act b
              on pay.fuid = b.fuid and pay.fbpid = b.fbpid and b.dt = '%(statdate)s'

            join dim.bpid_map bm
              on pay.fbpid = bm.fbpid
            where pay.dt >= '%(ld_30day_ago)s' and pay.dt < '%(ld_end)s'
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        select dt fdate,
            fgamefsk,
            coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
            coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
            coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
            coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
            coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
            coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
            retday,
            count(distinct fuid) retusernum
        from work.paid_user_actret_temp1_%(statdatenum)s
        where hallmode = %(hallmode)s
        group by dt,fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,
            fchannel_code,retday
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql ="""
        drop table if exists work.paid_user_actret_temp2_%(statdatenum)s;
        create table work.paid_user_actret_temp2_%(statdatenum)s as
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        insert overwrite table  dcnew.paid_user_actret
        partition( dt )
        select fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
        max(f1daycnt) f1daycnt,
        max(f2daycnt) f2daycnt,
        max(f3daycnt) f3daycnt,
        max(f4daycnt) f4daycnt,
        max(f5daycnt) f5daycnt,
        max(f6daycnt) f6daycnt,
        max(f7daycnt) f7daycnt,
        max(f14daycnt) f14daycnt,
        max(f30daycnt) f30daycnt,
        fdate dt
        from (
        select fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                if( retday=1, retusernum, 0 ) f1daycnt,
                if( retday=2, retusernum, 0 ) f2daycnt,
                if( retday=3, retusernum, 0 ) f3daycnt,
                if( retday=4, retusernum, 0 ) f4daycnt,
                if( retday=5, retusernum, 0 ) f5daycnt,
                if( retday=6, retusernum, 0 ) f6daycnt,
                if( retday=7, retusernum, 0 ) f7daycnt,
                if( retday=14, retusernum, 0 ) f14daycnt,
                if( retday=30, retusernum, 0 ) f30daycnt
            from work.paid_user_actret_temp2_%(statdatenum)s
            union all
            SELECT fdate,fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                   f1daycnt,
                   f2daycnt,
                   f3daycnt,
                   f4daycnt,
                   f5daycnt,
                   f6daycnt,
                   f7daycnt,
                   f14daycnt,
                   f30daycnt
            FROM  dcnew.paid_user_actret
            WHERE dt >= '%(ld_30day_ago)s'
              AND dt < '%(ld_end)s'
        ) tmp group by fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode
        """

        res = self.sql_exe(hql)
        if res != 0:return res

        hql ="""--最近三十天的数据放到同一个分区中，提高同步效率
        insert overwrite table dcnew.paid_user_actret partition(dt='3000-01-01')
            select fdate,fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                   f1daycnt,
                   f2daycnt,
                   f3daycnt,
                   f4daycnt,
                   f5daycnt,
                   f6daycnt,
                   f7daycnt,
                   f14daycnt,
                   f30daycnt
            from dcnew.paid_user_actret
            where dt >= '%(ld_30day_ago)s' and dt < '%(ld_end)s'
        """
        res = self.sql_exe(hql)
        if res != 0:return res



        res = self.sql_exe("""drop table if exists work.paid_user_actret_temp1_%(statdatenum)s;
                              drop table if exists work.paid_user_actret_temp2_%(statdatenum)s;""")
        if res != 0: return res


#愉快的统计要开始啦
a = agg_paid_user_actret(sys.argv[1:])
a()
