#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat
import service.sql_const as sql_const


class agg_reg_user_interval_actret(BaseStat):
    """新增用户，在其后一段时间内的留存
    """
    def create_tab(self):
        hql = """create table if not exists dcnew.reg_user_interval_actret
                (
                fdate date,
                fgamefsk bigint,
                fplatformfsk bigint,
                fhallfsk bigint,
                fsubgamefsk bigint,
                fterminaltypefsk bigint,
                fversionfsk bigint,
                fchannelcode bigint,

                f7daycnt bigint,
                f14daycnt bigint,
                f30daycnt bigint
                )
                partitioned by (dt date)"""
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res


    def stat(self):
        query = {'ld_30daylater':(datetime.datetime.strptime(self.stat_date, "%Y-%m-%d") + datetime.timedelta(days=30)).strftime('%Y-%m-%d')}
        query.update(PublicFunc.date_define(self.stat_date))

        res = self.hq.exe_sql("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dcnew.reg_user_interval_actret
        partition( dt)
        select
            fdate,
            fgamefsk,
            fplatformfsk,
            fhallfsk,
            fsubgamefsk,
            fterminaltypefsk,
            fversionfsk,
            fchannelcode,
            count(case when retday<=7  then fuid else null end) f7daycnt,
            count(case when retday<=14 then fuid else null end) f14daycnt,
            count(case when retday<=30 then fuid else null end) f30daycnt,
            fdate
        from
            (select
                fsignup_at fdate,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
                fuid,
                min(datediff(fdate, a.fsignup_at)) retday
            from dim.user_retain_array a
           where dt >= '%(ld_30dayago)s' and dt <= '%(ld_daybegin)s'
             and fsignup_at < fdate and fsignup_at >= '%(ld_30dayago)s'
           group by
                fsignup_at,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
                fuid
            ) a
        group by
             fdate,
             fgamefsk,
             fplatformfsk,
             fhallfsk,
             fsubgamefsk,
             fterminaltypefsk,
             fversionfsk,
             fchannelcode
        """% query

        res = self.hq.exe_sql(hql)
        if res != 0:
            return res


        hql = """
        insert overwrite table dcnew.reg_user_interval_actret partition(dt='3000-01-01')
        select fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
               f7daycnt,
               f14daycnt,
               f30daycnt
        from dcnew.reg_user_interval_actret
        where dt >= '%(ld_30dayago)s'
        and dt < '%(ld_daybegin)s'
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

#愉快的统计要开始啦
global statDate

if len(sys.argv) == 1:
    #没有输入参数的话,日期默认取昨天
    statDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d")
else:
    #从输入参数取,实际是第几个就取第几个
    args = sys.argv[1].split(',')
    statDate = args[0]


#生成统计实例
a = agg_reg_user_interval_actret(statDate)
a()
