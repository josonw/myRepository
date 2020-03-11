#-*- coding: UTF-8 -*-
# Author：AnsenWen
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class predict_day_fct(BaseStat):
    def create_tab(self):
        hql = """
            create table if not exists analysis.predict_day_fct
                (
                    fdate            date,
                    fpdate           date,
                    fgamefsk         bigint,
                    fplatformfsk     bigint,
                    fversionfsk      bigint,
                    ftype            varchar(100),
                    fvalue           bigint
                )
            partitioned by(dt date)
        """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

    def stat(self):
        """统计内容"""
        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)

        stat_date = datetime.datetime.strptime(query['ld_daybegin'], '%Y-%m-%d')
        pre_date = [datetime.datetime.strftime(stat_date + datetime.timedelta(days=i+1), '%Y-%m-%d') for i in range(30)]
        query.update({"pre_str": ", ".join('"' + d + '"' for d in pre_date)})

        res = self.hq.exe_sql("""use analysis; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        # 生成往前预测30天的日期的临时表
        hql = """
            drop table if exists analysis.predict_30day_tmp_%(num_begin)s;
            create table analysis.predict_30day_tmp_%(num_begin)s as
            select stack(30,
                %(pre_str)s
            ) as (fpdate)
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        # 临时表，游戏gpv信息，以及join30天日期

        hql = """
            drop table if exists analysis.predict_day_tmp_01_%(num_begin)s;
            create table analysis.predict_day_tmp_01_%(num_begin)s as
            select  '%(ld_daybegin)s' fdate,
                    fpdate,
                    fplatformfsk,
                    fgamefsk,
                    fversionfsk,
                    ftype
            from (
                    select *
                      from analysis.predict_factor_mid
                     where dt > '%(ld_7dayago)s'
                       and dt <= '%(ld_daybegin)s'
            ) a
            join analysis.predict_30day_tmp_%(num_begin)s c
              on 1 = 1
           group by fpdate, fplatformfsk, fgamefsk, fversionfsk, ftype
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        # 预测的因子，回归的参数
        hql = """
            drop table if exists analysis.predict_day_tmp_02_%(num_begin)s;
            create table analysis.predict_day_tmp_02_%(num_begin)s as
            select t1.fdate,
                   fpdate,
                   t1.fgamefsk,
                   t1.fplatformfsk,
                   t1.fversionfsk,
                   t1.ftype,
                   pmod(datediff(fpdate, '2014-06-01'), 7) fweekday,
                   (round((datediff(fpdate, t2.fdate) + 64) / 7, 0) + 1) ft,
                   flm_slope,
                   flm_intercept,
                   case pmod(datediff(fpdate, '2014-06-01'), 7)
                        when 1 then fweekfactor_01
                        when 2 then fweekfactor_02
                        when 3 then fweekfactor_03
                        when 4 then fweekfactor_04
                        when 5 then fweekfactor_05
                        when 6 then fweekfactor_06
                        when 0 then fweekfactor_07
                    end fweekfactor
            from analysis.predict_day_tmp_01_%(num_begin)s t1
            join (
                    select *
                      from analysis.predict_factor_mid
                     where dt > '%(ld_7dayago)s'
                       and dt <= '%(ld_daybegin)s'
                 )  t2
              on t1.fgamefsk = t2.fgamefsk
             and t1.fplatformfsk = t2.fplatformfsk
             and t1.fversionfsk = t2.fversionfsk
             and t1.ftype = t2.ftype
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        # 预测未来30天
        hql = """
            insert overwrite table analysis.predict_day_fct
            partition(dt='%(ld_daybegin)s')
            select fdate,
                   fpdate,
                   fgamefsk,
                   fplatformfsk,
                   fversionfsk,
                   ftype,
                   case when (flm_slope * ft + flm_intercept) * fweekfactor < 0 then 0
                    else (flm_slope * ft + flm_intercept) * fweekfactor end fvalue
            from analysis.predict_day_tmp_02_%(num_begin)s
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
            drop table if exists analysis.predict_30day_tmp_%(num_begin)s;
            drop table if exists analysis.predict_day_tmp_01_%(num_begin)s;
            drop table if exists analysis.predict_day_tmp_02_%(num_begin)s;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

if __name__ == "__main__":
    stat_date = get_stat_date()

    #生成统计实例
    a = predict_day_fct(stat_date)
    a()
