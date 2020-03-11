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



class agg_act_user_actret_month(BaseStatModel):
    """活跃用户，活跃留存，自然月
    """

    def create_tab(self):
        hql = """create table if not exists dcnew.act_user_actret_month
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fhallfsk bigint,
                fsubgamefsk bigint,
                fterminaltypefsk bigint,
                fversionfsk bigint,
                fchannelcode bigint,
                fmonthaucnt bigint,
                f1monthcnt bigint,
                f2monthcnt bigint,
                f3monthcnt bigint
                )
                partitioned by (dt date)"""
        res = self.sql_exe(hql)
        if res != 0: return res

        return res

    def stat(self):
        # self.hq.debug = 0
        extend_group = {
                     'fields':[ 'fdate','retday'],
                     'groups':[ [1,1]]
                     }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql= """
        drop table if exists work.act_user_actret_month_temp_%(statdatenum)s;
        create table if not exists work.act_user_actret_month_temp_%(statdatenum)s
        as
        select
        fdate, fbpid, fuid, fgame_id, fchannel_code,
        fgamefsk, fplatformfsk, fhallfsk, fterminaltypefsk, fversionfsk, hallmode,
        round(datediff('%(ld_month_begin)s', fdate)/30) retday,
        max(fdate) over(PARTITION BY fbpid, fuid order by fgame_id) flag
        from
        (
            select
            case when a.dt>='%(ld_3month_ago_begin)s' and a.dt < '%(ld_3month_ago_end)s' then '%(ld_3month_ago_begin)s'
                 when a.dt>='%(ld_2month_ago_begin)s' and a.dt < '%(ld_2month_ago_end)s' then '%(ld_2month_ago_begin)s'
                 when a.dt>='%(ld_1month_ago_begin)s' and a.dt < '%(ld_1month_ago_end)s' then '%(ld_1month_ago_begin)s'
                 when a.dt>='%(ld_month_begin)s' and a.dt < '%(ld_month_end)s' then '%(ld_month_begin)s'
            end fdate,
            a.fbpid, a.fuid, a.fgame_id, a.fchannel_code,
            bm.fgamefsk, bm.fplatformfsk, bm.fhallfsk, bm.fterminaltypefsk, bm.fversionfsk, bm.hallmode
            from dim.user_act a
            join dim.bpid_map bm
              on a.fbpid = bm.fbpid
            where a.dt>='%(ld_3month_ago_begin)s' and a.dt < '%(ld_month_end)s'
            group by
            a.fbpid, a.fuid, a.fgame_id, a.fchannel_code,
            bm.fgamefsk, bm.fplatformfsk, bm.fhallfsk, bm.fterminaltypefsk, bm.fversionfsk, bm.hallmode,
            case when a.dt>='%(ld_3month_ago_begin)s' and a.dt < '%(ld_3month_ago_end)s' then '%(ld_3month_ago_begin)s'
                 when a.dt>='%(ld_2month_ago_begin)s' and a.dt < '%(ld_2month_ago_end)s' then '%(ld_2month_ago_begin)s'
                 when a.dt>='%(ld_1month_ago_begin)s' and a.dt < '%(ld_1month_ago_end)s' then '%(ld_1month_ago_begin)s'
                 when a.dt>='%(ld_month_begin)s' and a.dt < '%(ld_month_end)s' then '%(ld_month_begin)s'
            end
        ) a
        """

        res = self.sql_exe(hql)
        if res != 0: return res

        hql="""
        select  a.fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(a.fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(a.fchannel_code,%(null_int_group_rule)d) fchannelcode,
                retday,
                count(distinct a.fuid) usernum,
                count(distinct case when a.flag = '%(ld_month_begin)s' then a.fuid else null end ) retusernum
        from work.act_user_actret_month_temp_%(statdatenum)s a
        where hallmode = %(hallmode)s
        group by fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,
            fchannel_code,fdate,retday
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql = """
        drop table if exists work.agg_user_retained_month_temp_%(statdatenum)s;
        create table work.agg_user_retained_month_temp_%(statdatenum)s as
         %(sql_template)s
        """
        res =  self.sql_exe(hql)
        if res != 0: return res


        hql = """
        insert overwrite table dcnew.act_user_actret_month
        partition( dt )
        select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
            max(fmonthaucnt) fmonthaucnt,
            max(f1monthcnt) f1monthcnt,
            max(f2monthcnt) f2monthcnt,
            max(f3monthcnt) f3monthcnt,
            fdate dt
        from (
            select fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
                usernum fmonthaucnt,
                if( retday=1, retusernum, 0 ) f1monthcnt,
                if( retday=2, retusernum, 0 ) f2monthcnt,
                if( retday=3, retusernum, 0 ) f3monthcnt
            from work.agg_user_retained_month_temp_%(statdatenum)s

            union all

            select cast(fdate as string),fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
            0 fmonthaucnt, f1monthcnt, f2monthcnt, f3monthcnt
            from dcnew.act_user_actret_month
            where dt >= '%(ld_3month_ago_begin)s' and dt < '%(ld_end)s'

        ) tmp group by fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode
        """

        res = self.sql_exe(hql)
        if res != 0: return res


        hql = """
        insert overwrite table dcnew.act_user_actret_month partition(dt='3000-01-01')
        select
        fdate,fgamefsk,fplatformfsk,fhallfsk,fsubgamefsk,fterminaltypefsk,fversionfsk,fchannelcode,
        fmonthaucnt,
        f1monthcnt,
        f2monthcnt,
        f3monthcnt
        from dcnew.act_user_actret_month
        where dt >= '%(ld_3month_ago_begin)s' and dt < '%(ld_end)s'
        """
        res = self.sql_exe(hql)
        if res != 0: return res


        res = self.sql_exe("""
            drop table if exists work.agg_user_retained_month_temp_%(statdatenum)s;
            drop table if exists work.act_user_actret_month_temp_%(statdatenum)s;
        """)
        if res != 0: return res

        return res



#愉快的统计要开始啦
#生成统计实例
a = agg_act_user_actret_month(sys.argv[1:])
a()
