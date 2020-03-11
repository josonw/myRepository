#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel


class agg_game_activity_payret(BaseStatModel):
    """ 游戏活动付费留存结果表"""
    def create_tab(self):
        hql = """
        create table if not exists dcnew.game_activity_payret
        (
            fdate                       date,
            fgamefsk                    bigint,
            fplatformfsk                bigint,
            fhallfsk                    bigint,
            fsubgamefsk                 bigint,
            fterminaltypefsk            bigint,
            fversionfsk                 bigint,
            fchannelcode                bigint,
            fact_id                     varchar(50),
            fusernum                    bigint,
            f1daycnt                    bigint,
            f2daycnt                    bigint,
            f3daycnt                    bigint,
            f4daycnt                    bigint,
            f5daycnt                    bigint,
            f6daycnt                    bigint,
            f7daycnt                    bigint,
            f14daycnt                   bigint,
            f30daycnt                   bigint
        )
        partitioned by (dt date)
        """
        result = self.sql_exe(hql)
        if result != 0: return result


    def stat(self):
        """ 重要部分，统计内容 """
        hql_list = []
        extend_group = {'fields':['fdate', 'fact_id'],
                        'groups':[[1, 1]]
                       }

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        drop table if exists work.game_activity_payret_%(statdatenum)s;
        create table work.game_activity_payret_%(statdatenum)s as
            select a.dt fdate, b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk, b.hallmode,
                    a.fbpid, a.fuid, c.fuid cfuid, a.fact_id, datediff( c.dt,cast(a.dt as date)) retday,
                    %(null_int_report)d fgame_id, %(null_int_report)d fchannel_code
              from stage.game_activity_stg a
              left join dim.user_pay_day c
                on a.fbpid = c.fbpid
               and a.fuid = c.fuid
               and c.dt='%(statdate)s'
              join dim.bpid_map b
                on a.fbpid=b.fbpid
             where (a.dt >= '%(ld_7day_ago)s' and a.dt < '%(ld_end)s') or a.dt='%(ld_14day_ago)s' or a.dt='%(ld_30day_ago)s'
             group by a.dt, b.fgamefsk, b.fplatformfsk, b.fhallfsk, b.fterminaltypefsk, b.fversionfsk, b.hallmode,
                    a.fbpid, a.fuid, c.fuid, a.fact_id, datediff( c.dt,cast(a.dt as date))
        """
        res = self.sql_exe(hql)
        if res != 0:return res



        hql = """
         select fdate,
                fgamefsk,
                coalesce(fplatformfsk,%(null_int_group_rule)d) fplatformfsk,
                coalesce(fhallfsk,%(null_int_group_rule)d) fhallfsk,
                coalesce(fgame_id,%(null_int_group_rule)d) fsubgamefsk,
                coalesce(fterminaltypefsk,%(null_int_group_rule)d) fterminaltypefsk,
                coalesce(fversionfsk,%(null_int_group_rule)d) fversionfsk,
                coalesce(fchannel_code,%(null_int_group_rule)d) fchannelcode,
                fact_id,
                max(retday) retday,
                count(distinct fuid ) fusernum,
                count(distinct cfuid ) retusernum
           from work.game_activity_payret_%(statdatenum)s a
          where hallmode = %(hallmode)s
          group by fdate, fgamefsk,fplatformfsk,fhallfsk,fgame_id,fterminaltypefsk,fversionfsk,fchannel_code,
                fact_id
        """
        self.sql_template_build(sql=hql, extend_group=extend_group)

        hql ="""
        drop table if exists work.game_activity_payret2_%(statdatenum)s;
        create table work.game_activity_payret2_%(statdatenum)s as
        %(sql_template)s
        """
        res = self.sql_exe(hql)
        if res != 0:return res


        hql = """
        insert overwrite table dcnew.game_activity_payret
        partition (dt)
        select fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
               fact_id,
            max(fusernum) fusernum,
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
            select cast(fdate as date) fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                fact_id, fusernum,
                if( retday=1, retusernum, 0 ) f1daycnt,
                if( retday=2, retusernum, 0 ) f2daycnt,
                if( retday=3, retusernum, 0 ) f3daycnt,
                if( retday=4, retusernum, 0 ) f4daycnt,
                if( retday=5, retusernum, 0 ) f5daycnt,
                if( retday=6, retusernum, 0 ) f6daycnt,
                if( retday=7, retusernum, 0 ) f7daycnt,
                if( retday=14, retusernum, 0 ) f14daycnt,
                if( retday=30, retusernum, 0 ) f30daycnt
            from work.game_activity_payret2_%(statdatenum)s
            union all
            select  fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    fact_id, fusernum,
                    f1daycnt,f2daycnt,f3daycnt,f4daycnt,f5daycnt,f6daycnt,f7daycnt,f14daycnt,f30daycnt
            from dcnew.game_activity_payret
            where dt >= '%(ld_30day_ago)s' and dt < '%(ld_end)s'
               ) tmp group by fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                       fact_id
        """
        res = self.sql_exe(hql)
        if res != 0:return res



        hql = """--最近三十天的数据放到同一个分区中，提高同步效率
        insert overwrite table dcnew.game_activity_payret partition(dt='3000-01-01')
                   select fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    fact_id, fusernum,
                    f1daycnt,f2daycnt,f3daycnt,f4daycnt,f5daycnt,f6daycnt,f7daycnt,f14daycnt,f30daycnt
            from dcnew.game_activity_payret
            where dt >= '%(ld_30day_ago)s' and dt < '%(ld_end)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


        hql = """
        drop table if exists work.game_activity_payret_%(statdatenum)s;
        drop table if exists work.game_activity_payret2_%(statdatenum)s;
        """
        res = self.sql_exe(hql)
        if res != 0:return res

        return res



#生成统计实例
a = agg_game_activity_payret(sys.argv[1:])
a()
