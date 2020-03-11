#-*- coding: UTF-8 -*-
# Author：AnsenWen
import sys
import os
import datetime
import numpy as np
import pandas as pd
from pandas import Series, DataFrame

# 时间如有缺漏，补全数据
def ComplementData(df):
    dates = pd.to_datetime(df['fdate'])
    df.loc[:, 'fdate'] = dates
    dindex = pd.date_range(min(dates), max(dates), freq = 'D')
    # 增加星期X
    weekdays = Series(dindex).apply(lambda x: x.weekday()) + 1
    # 增加第X星期
    start_indx = weekdays[weekdays == 1].index[0]
    n = len(weekdays)
    wname = np.arange(1, n / 7 + 2).repeat(7)
    if start_indx > 0:
        weekname = np.hstack((np.array([1]).repeat(start_indx), wname + 1))[0 : n]
    else:
        weekname = wname[0 : n]
    # 合并数据集
    df2 = DataFrame({'date':dindex, 'weekday': weekdays, 'weekname': weekname})
    df3 = pd.merge(df2, df, left_on = 'date', right_on = 'fdate',how = 'left')
    df4 = df3.loc[:, ('date', 'weekday', 'weekname', 'fvalue')]
    df4.loc[:, 'fvalue'] = df3.loc[:, 'fvalue'].interpolate()
    return(df4)

# 通过箱线图法确定判断异常值的阈值
def GetOutlierThreshold(s):
    q1 =  s.quantile(0.25)
    q3 =  s.quantile(0.75)
    iqr = q3 - q1
    limit = q3 + 1.5 * iqr
    return(limit)

# 线性回归
def LinearFit(y):
    t = np.arange(1, len(y) + 1)
    A = np.array([t, np.ones(len(y))])
    k, b = np.linalg.lstsq(A.T,y)[0]
    return(k, b)

# 计算因子
def ComputeFactor(df):
    df2 = ComplementData(df)
    week_mean = df2.loc[:,('weekname','fvalue')].groupby('weekname').mean()
    k,b = LinearFit(week_mean)
    df5 = DataFrame({'weekname':week_mean.index, 'week_mean':week_mean.loc[:,'fvalue']})
    df6 = pd.merge(df2, df5, on = 'weekname', how = 'left')
    df6['factor'] = df6.loc[:, 'fvalue'] / df6.loc[:, 'week_mean']
    weekfactor = df6.loc[:,('weekday','factor')].groupby('weekday').mean().reset_index()

    # 预测
    t = np.arange(1, len(week_mean) + 1)
    week_fitted = DataFrame({'week_fitted':t * k[0] + b[0], 'weekname':t})
    df7 = pd.merge(df6, week_fitted, on = 'weekname', how = 'left')
    df7['day_fitted'] = df7.loc[:, 'factor'] * df7.loc[:, 'week_fitted']
    df7.loc[df7['day_fitted'] <= 0, ['day_fitted']] = 0.000001
    df7.loc[df7['fvalue'] <= 0, ['fvalue']] = 0.000001
    df7['res01'] = df7.loc[:, 'fvalue'] / df7.loc[:, 'day_fitted']
    df7['res02'] = df7.loc[:, 'day_fitted'] / df7.loc[:, 'fvalue']

    # 异常阈值
    threshold01 = GetOutlierThreshold(df7.loc[:,'res01'])
    threshold02 = GetOutlierThreshold(df7.loc[:,'res02'])

    # 剔除异常值
    outlier = df7.loc[:, 'fvalue'][(df7.loc[:, 'res01']> threshold01) | (df7.loc[:, 'res02']> threshold02)]
    df7.loc[outlier.index, 'fvalue'] = df7.loc[outlier.index, 'day_fitted']

    # 剔除异常值后，重新计算
    week_mean_new = df7.loc[:,('weekname','fvalue')].groupby('weekname').mean()
    k_new,b_new = LinearFit(week_mean_new)
    df8 = DataFrame({'weekname':week_mean_new.index, 'new_mean':week_mean_new.loc[:,'fvalue']})
    df9 = pd.merge(df7, df8, on = 'weekname', how = 'left')

    df9['factor'] = df9.loc[:, 'fvalue'] / df9.loc[:, 'new_mean']
    weekfactor = df9.loc[:,('weekday','factor')].groupby('weekday').mean().reset_index()

    # 输出因子
    factor_indx = ['flm_intercept','flm_slope']
    for i in range(1, 8):
        factor_indx.append('fweekfactor_0' + str(i))

    factor_indx.append('fthreshold_01')
    factor_indx.append('fthreshold_02')
    output_factor = Series([b_new[0], k_new[0]] + list(weekfactor.loc[:,'factor'].values) + [threshold01, threshold02],
                           index = factor_indx)
    return(output_factor)

# 通过groupby计算各bpid的因子
def GetMultFactor(data, fdate, type):
    grouped = data.groupby('fbpid')
    factor = grouped.apply(ComputeFactor).reset_index()
    nrow = len(factor.index)
    result = DataFrame({'fdate' : [fdate] * nrow, 'ftype': [type] * nrow})
    result = pd.concat([result, factor],axis = 1)
    result['fdate'] = result['fdate'].astype('str')
    result['ftype'] = result['ftype'].astype('str')
    result['fbpid'] = result['fbpid'].astype('str')
    return(result)

# dataframe结果转化成str
def TransDataToStr(df):
    data_list = df.values.tolist()
    d_list = []
    for dl in data_list:
        for d in dl:
            d_list.append(d)
    data_str = ", ".join('"' + str(d) + '"' for d in d_list)
    return data_str
