#-*- coding: UTF-8 -*- 
import datetime
import numpy as np
import pandas as pd
from pandas import Series, DataFrame
import math
import os
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStat import BasePGStat, get_stat_date, pg_db
from PublicFunc import PublicFunc
from predict_roi_sql import sql_template 

class predict_roi_month_game_factor():
    """ROI预测的参数因子"""
    def __init__(self, stat_date, pg_db):
        self.pg_con = pg_db._db
        self.cursor = self.pg_con.cursor()
        self.stat_date = stat_date
        self.sql_dict = PublicFunc.date_define(self.stat_date)

    def getPgData(self):
        sql = sql_template["roi_month_game"] % self.sql_dict
        print sql
        # datas = self.query(sql)
        # df = pd.DataFrame(datas, columns=['fmonth','fgame_id', 'fgame', 'fbelong_group', 'fdru_month', 'fcost','fdip'])
        self.data = pd.read_sql(sql, self.pg_con)


    # 计算预测因子
    def caculateParam(self, data):
        data = self.data
        # today = datetime.datetime.today()
        try:
            fgame_id = data['fgame_id'].iloc[0]
            fgame = data['fgame'].iloc[0]
            fbelong_group = data['fbelong_group'].iloc[0]
        except:
            pass
        else:
            ffirst_roi = []
            fmonth = []
            fexp = []
            mname = sorted(np.unique(data.loc[:,'fmonth']))
            if len(mname) >= 4 :
                for m in mname:
                    sdata = data[data.fmonth == m].sort(['fdru_month'])
                    try:
                        roi = sdata[sdata.fdru_month == 0]['fdip'] / sdata[sdata.fdru_month == 0]['fcost']
                        if m != mname[-1]:
                            y = sdata['fdip'].map(lambda x : np.log(x))
                            t = np.array(sdata['fdru_month'])
                            A = np.array([t, np.ones(len(y))])
                            k, b = np.linalg.lstsq(A.T,y)[0]
                        else:
                            k = np.mean(fexp[-3 : -1])
                    
                        ffirst_roi.append(roi.values[0])
                        fmonth.append(m + '-01')
                        fexp.append(None if math.isnan(k) else k)
                    except:
                        continue                
                nrow = len(mname)
                fdate = self.stat_date
                d = {'fdate': fdate, 'fgame_id': fgame_id, 'fgame': fgame, 'fbelong_group': fbelong_group,
                'fsignup_month':fmonth, 'ffirst_roi':ffirst_roi, 'fexp_slope':fexp,}
                param = DataFrame(d, columns = ['fdate','fgame_id', 'fgame', 'fbelong_group', 'fsignup_month', 
                'ffirst_roi', 'fexp_slope'])
                result = param[param['ffirst_roi'] != float('inf')]
                return result
                

    def getResult(self):
        self.result = self.data.groupby(['fgame', 'fbelong_group']).apply(self.caculateParam).reset_index(drop = True)

    # 数据导入数据库       
    def outputDataToPg(self):
        result = self.result
        ## have to delete the data produced in this month
        del_sql = """
                delete from analysis.prediction_roi_mid 
                 where fdate >= to_date('%(ld_monthbegin)s', 'yyyy-mm-dd') 
                   and fdate < to_date('%(ld_monthend)s', 'yyyy-mm-dd')""" % self.sql_dict
        print del_sql
        self.cursor.execute(del_sql)
        self.pg_con.commit()
        
        ## insert into database
        hql = """
            insert into analysis.prediction_roi_mid
            (   fdate, 
                fgame_id,
                fgame,
                fbelong_group,
                fsignup_month,
                ffirst_ROI,
                fexp_slope
            )
            values(%s, %s, %s, %s, %s, %s, %s)"""
        print hql
        result['fdate'] = result['fdate'].astype('datetime64')
        result['fsignup_month'] = result['fsignup_month'].astype('datetime64')
        result['fgame_id'] = result['fgame_id'].astype('float')
        self.cursor.executemany(hql, result.values.tolist())
        self.pg_con.commit()  
            
    def __call__(self):
        self.getPgData()
        self.getResult()
        self.outputDataToPg()

if __name__ == '__main__':
    stat_date = get_stat_date()
    p = predict_roi_month_game_factor(stat_date, pg_db)
    p()

    


