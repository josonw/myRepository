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

class predict_outlier_fct(BaseStat):
    def create_tab(self):
        hql = """
            create table if not exists analysis.predict_outlier_fct
            (
                fdate            date,
                fgamefsk         bigint,
                fplatformfsk     bigint,
                fversionfsk      bigint,
                ftype            varchar(100),
                fres_01          decimal(30, 4),
                fres_02          decimal(30, 4),
                foutlier_type    tinyint
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

        res = self.hq.exe_sql("""use analysis; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        # 临时表，计算收入误差比例
        hql = """
            drop table if exists analysis.predict_outlier_tmp_01_%(num_begin)s;
            create table analysis.predict_outlier_tmp_01_%(num_begin)s as
            select t1.fdate,
                   t1.fpdate,
                   t1.fgamefsk,
                   t1.fplatformfsk,
                   t1.fversionfsk,
                   t1.ftype,
                   case when t1.fvalue = 0 then null else t2.fincome / t1.fvalue end fres_01,
                   case when t2.fincome = 0 then null else t1.fvalue / t2.fincome end fres_02
              from (select *
                     from analysis.predict_day_fct
                    where dt = '%(ld_1dayago)s'
                      and fpdate = '%(ld_daybegin)s'
                      and ftype = 'dip') t1
              join analysis.user_payment_fct t2
                on t1.fgamefsk = t2.fgamefsk
               and t1.fplatformfsk = t2.fplatformfsk
               and t1.fversionfsk = t2.fversionfsk
               and t1.fpdate = t2.fdate

        """ %  query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        # 临时表，计算活跃误差比例
        hql = """
            drop table if exists analysis.predict_outlier_tmp_02_%(num_begin)s;
            create table analysis.predict_outlier_tmp_02_%(num_begin)s as
            select t1.fdate,
                   t1.fpdate,
                   t1.fgamefsk,
                   t1.fplatformfsk,
                   t1.fversionfsk,
                   t1.ftype,
                   case when t1.fvalue = 0 then null else t2.factcnt / t1.fvalue end fres_01,
                   case when t2.factcnt = 0 then null else t1.fvalue / t2.factcnt end fres_02
             from (select *
                     from analysis.predict_day_fct
                    where dt = '%(ld_1dayago)s'
                      and fpdate = '%(ld_daybegin)s'
                      and ftype = 'dau') t1
             join analysis.user_true_active_fct t2
               on t1.fgamefsk = t2.fgamefsk
              and t1.fplatformfsk = t2.fplatformfsk
              and t1.fversionfsk = t2.fversionfsk
              and t1.fpdate = t2.fdate
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
            drop table if exists analysis.predict_outlier_tmp_03_%(num_begin)s;
            create table analysis.predict_outlier_tmp_03_%(num_begin)s as
            select ftype,
                   fthreshold_01,
                   fthreshold_02,
                   fgamefsk,
                   fplatformfsk,
                   fversionfsk
              from analysis.predict_factor_mid
             where dt > '%(ld_7dayago)s'
               and dt <= '%(ld_daybegin)s'
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        #判断异常，1是偏小，2是偏大
        hql = """
            insert overwrite table analysis.predict_outlier_fct
            partition (dt='%(ld_daybegin)s')
              select '%(ld_daybegin)s' fdate,
                     a.fgamefsk,
                     a.fplatformfsk,
                     a.fversionfsk,
                     a.ftype,
                     a.fres_01,
                     a.fres_02,
                     case
                       when a.fres_01 > b.fthreshold_01 then 1
                       when a.fres_02 > b.fthreshold_02 then 2
                       else 0
                     end foutlier_type
                from (select *
                        from analysis.predict_outlier_tmp_01_%(num_begin) s
                      union all
                      select *
                        from analysis.predict_outlier_tmp_02_%(num_begin) s) a
                join analysis.predict_outlier_tmp_03_%(num_begin) s b
                  on a.fgamefsk = b.fgamefsk
                 and a.fplatformfsk = b.fplatformfsk
                 and a.fversionfsk = b.fversionfsk
                 and a.ftype = b.ftype
          """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
            drop table if exists analysis.predict_outlier_tmp_01_%(num_begin)s;
            drop table if exists analysis.predict_outlier_tmp_02_%(num_begin)s;
            drop table if exists analysis.predict_outlier_tmp_03_%(num_begin)s;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res


if __name__ == "__main__":

    stat_date = get_stat_date()
    #生成统计实例
    a = predict_outlier_fct(stat_date)
    a()

