#-*- coding: UTF-8 -*-
# Author：AnsenWen
import sys
import os
import datetime
import numpy as np
import pandas as pd
from pandas import Series, DataFrame
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date
from predict_factor_function import *

# 继承Hive运算的父类
class predict_factor_mid(BaseStat):
    def create_tab(self):
        """
        建立预测因子中间表，存回归的参数和周期变动因子
        ftype：标识是活跃还是收入预测
        flm_intercept：  线性回归的截距
        flm_slope：      线性回归的
        fweekfactor_01： 周一的周期变动因子
        fweekfactor_02： 周二的周期变动因子
        fweekfactor_03： 周三的周期变动因子
        fweekfactor_04： 周四的周期变动因子
        fweekfactor_05： 周五的周期变动因子
        fweekfactor_06： 周六的周期变动因子
        fweekfactor_07： 周日的周期变动因子
        fthreshold_01：  判断异常大值的阈值
        fthreshold_02：  判断异常小值的阈值
        """
        hql = """
            create table if not exists analysis.predict_factor_mid
            (
                  fdate          date,
                  fgamefsk       bigint,
                  fplatformfsk   bigint,
                  fversionfsk    bigint,
                  ftype          varchar(16),
                  flm_intercept  decimal(30, 4),
                  flm_slope      decimal(30, 4),
                  fweekfactor_01 decimal(30, 4),
                  fweekfactor_02 decimal(30, 4),
                  fweekfactor_03 decimal(30, 4),
                  fweekfactor_04 decimal(30, 4),
                  fweekfactor_05 decimal(30, 4),
                  fweekfactor_06 decimal(30, 4),
                  fweekfactor_07 decimal(30, 4),
                  fthreshold_01  decimal(30, 4),
                  fthreshold_02  decimal(30, 4)
            )
            partitioned by(dt date)
        """
        res = self.hq.exe_sql(hql)
        if res !=0: return res

        return res

    def stat(self):
        """统计内容"""
        dates_dict = PublicFunc.date_define(self.stat_date)
        ld_start = (datetime.datetime.strptime(stat_date, '%Y-%m-%d') + datetime.timedelta(days=-63)).strftime('%Y-%m-%d')
        query = {'ld_start': ld_start}
        query.update(dates_dict)

        res = self.hq.exe_sql("""use analysis; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        # hive获取各bpid的收入历史数据，先建临时表
        hql = """
            drop table if exists analysis.predict_factor_tmp_01_%(num_begin)s;
            create table analysis.predict_factor_tmp_01_%(num_begin)s as
            select fdate, t3.fbpid, sum(fincome) fvalue
              from analysis.user_payment_fct t1
              join analysis.bpid_platform_game_ver_map t2
                on t1.fgamefsk = t2.fgamefsk
               and t1.fplatformfsk = t2.fplatformfsk
               and t1.fversionfsk = t2.fversionfsk
               and t2.fgamename != '汇总数据'
              join (select fbpid
                      from analysis.user_payment_fct a
                      join analysis.bpid_platform_game_ver_map b
                        on a.fgamefsk = b.fgamefsk
                       and a.fplatformfsk = b.fplatformfsk
                       and a.fversionfsk = b.fversionfsk
                       and b.fgamename != '汇总数据'
                     where dt >= '%(ld_start)s'
                     group by fbpid
                    having count(fdate) >= 50
                       and sum(fincome) > 100) t3
                on t2.fbpid = t3.fbpid
             where dt >= '%(ld_start)s'
               and dt <= '%(ld_daybegin)s'
             group by fdate, t3.fbpid
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        # 从临时表获取数据，并转成list格式
        res = self.hq.query("""select * from analysis.predict_factor_tmp_01_%(num_begin)s""" % query)
        if not res:
            return "query failed"
        else:
            data = []
            for r in res:
                data.append(r)

        # 转成DataFrame格式
        df = pd.DataFrame(data, columns=['fdate', 'fbpid', 'fvalue'])
        df['fvalue'] = df['fvalue'].astype('float')

        # 通过时间序列法，得出各bpid收入预测的因子
        result_dip = GetMultFactor(df, stat_date, 'dip')

        # 从hive获取各bpid的活跃历史数据，先把历史数据存进临时表
        hql = """
            drop table if exists analysis.predict_factor_tmp_02_%(num_begin)s;
            create table analysis.predict_factor_tmp_02_%(num_begin)s as
            select fdate, t3.fbpid, sum(factcnt) fvalue
              from analysis.user_true_active_fct t1
              join analysis.bpid_platform_game_ver_map t2
                on t1.fgamefsk = t2.fgamefsk
               and t1.fplatformfsk = t2.fplatformfsk
               and t1.fversionfsk = t2.fversionfsk
               and t2.fgamename != '汇总数据'
              join (select fbpid
                      from analysis.user_true_active_fct a
                      join analysis.bpid_platform_game_ver_map b
                        on a.fgamefsk = b.fgamefsk
                       and a.fplatformfsk = b.fplatformfsk
                       and a.fversionfsk = b.fversionfsk
                       and b.fgamename != '汇总数据'
                     where dt >= '%(ld_start)s'
                     group by fbpid
                    having count(fdate) >= 50
                       and sum(factcnt) > 1000) t3
                on t2.fbpid = t3.fbpid
             where dt >= '%(ld_start)s'
               and dt <= '%(ld_daybegin)s'
             group by fdate, t3.fbpid
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        # 从临时表获取数据，并转成list
        res = self.hq.query("""select * from analysis.predict_factor_tmp_02_%(num_begin)s""" % query)
        if not res:
            return "query failed"
        else:
            data = []
            for r in res:
                data.append(r)

        # 将list转成DataFrame
        df = pd.DataFrame(data, columns=['fdate', 'fbpid', 'fvalue'])
        df['fvalue'] = df['fvalue'].astype('float')

        # 通过时间序列法，得出各bpid活跃预测的因子
        result_dau = GetMultFactor(df, stat_date, 'dau')

        # 将结果拼接成字符串，插入Hive
        row_cnt = len(result_dip.index) + len(result_dau.index)
        result_str = TransDataToStr(result_dip) + "," + TransDataToStr(result_dau)
        query.update({'row_cnt': row_cnt, 'result_str': result_str})

        # 将预测因子结果先插入临时表
        hql = """
            drop table if exists analysis.predict_factor_tmp_03_%(num_begin)s;
            create table analysis.predict_factor_tmp_03_%(num_begin)s as
            select stack(%(row_cnt)s,
             %(result_str)s
            ) as ( fdate,
                   ftype,
                   fbpid,
                   flm_intercept,
                   flm_slope,
                   fweekfactor_01,
                   fweekfactor_02,
                   fweekfactor_03,
                   fweekfactor_04,
                   fweekfactor_05,
                   fweekfactor_06,
                   fweekfactor_07,
                   fthreshold_01,
                   fthreshold_02)
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        # 将临时表的数据插入结果表
        hql = """
            insert overwrite table analysis.predict_factor_mid
            partition(dt='%(ld_daybegin)s')
            select fdate,
                   fgamefsk,
                   fplatformfsk,
                   fversionfsk,
                   ftype,
                   flm_intercept,
                   flm_slope,
                   fweekfactor_01,
                   fweekfactor_02,
                   fweekfactor_03,
                   fweekfactor_04,
                   fweekfactor_05,
                   fweekfactor_06,
                   fweekfactor_07,
                   fthreshold_01,
                   fthreshold_02
            from analysis.predict_factor_tmp_03_%(num_begin)s a
            join analysis.bpid_platform_game_ver_map b
             on a.fbpid = b.fbpid
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        # 干掉临时工
        hql = """
            drop table if exists analysis.predict_factor_tmp_01_%(num_begin)s;
            drop table if exists analysis.predict_factor_tmp_02_%(num_begin)s;
            drop table if exists analysis.predict_factor_tmp_03_%(num_begin)s;
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res


if __name__ == '__main__':
    stat_date = get_stat_date()
    wday = datetime.datetime.strptime(stat_date, '%Y-%m-%d').weekday()
    """周一的时候才运行"""
    if wday == 0:
        p = predict_factor_mid(stat_date)
        p()

