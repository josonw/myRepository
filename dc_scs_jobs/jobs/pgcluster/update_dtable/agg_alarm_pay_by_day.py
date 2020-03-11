#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
import collections
from dateutil.relativedelta import *
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BasePGCluster, get_stat_date, pg_db

def is_number(data):
    err_code = 0
    try:
        float(data)
    except Exception, e:
        err_code = -3
    return err_code


def divisor_by(a,b):
    """ a/b
    """
    reslut = 0
    if is_number(a)== 0 and is_number(b)==0:
        if b != 0:
            reslut = round( float(a)/float(b), 4)
    return reslut


def get_growth_rate(a,b):
    return round((divisor_by((a-b), b) * 100),1)


def get_tb_hb(data,date, type = 'd'):
    result = {'hb':0,'tb':0}
    d = round(data.get(date, 0), 2) if data.get(date, 0) % 1 else data.get(date, 0)
    if type == 'd':
        hb_day_data = data.get(PublicFunc.add_days(date, -1), 0)
        tb_day_data = data.get(PublicFunc.add_days(date, -7), 0)
    elif type == 'm':
        hb_day_data = data.get(PublicFunc.add_months(date, -1), 0)
        tb_day_data = data.get(PublicFunc.add_months(date, -3), 0)
    hb_per = get_growth_rate(d, hb_day_data)
    tb_per = get_growth_rate(d, tb_day_data)
    result['hb'] = hb_per
    result['tb'] = tb_per

    return result


class agg_alarm_pay_by_day(BasePGCluster):
    def __init__(self, stat_date=datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d"), eid = 0):
        BasePGCluster.__init__(self, stat_date)
        self.stat_date = stat_date
        self.metadata = {}
        self.con_data = {}
        self.insert_data = []
        self.dims = ['fmoney','fpayuser_cnt','forder','ocr']



    def get_base_data(self):
        #sql里默认取了七天的数据，用来算后面的n天内（n=5）的均值
        sql = """SELECT to_char(fdate,'YYYY-MM-DD') fdate,
                       fgamefsk,
                       fprovince,
                       fm_id,
                       round(coalesce(sum(fmoney),0),2) fmoney,
                       coalesce(sum(fpayuser_cnt),0) fpayuser_cnt,
                       coalesce(sum(forder),0) forder,
                       CASE coalesce(sum(forder),0)
                           WHEN 0 THEN 0
                           ELSE coalesce(sum(fpayuser_cnt),0)/coalesce(sum(forder),0)*100
                       END ocr
                FROM analysis.pay_channel_province_fct a
                WHERE fdate >= to_date('%(ld_7dayago)s', 'yyyy-mm-dd')
                  AND fdate <= to_date('%(ld_end)s', 'yyyy-mm-dd')
                  AND a.fgamefsk !=4118194431
                GROUP BY to_char(fdate,'YYYY-MM-DD'),
                         fgamefsk,
                         fprovince,
                         fm_id
                UNION
                SELECT to_char(fdate,'YYYY-MM-DD') fdate,
                       0 fgamefsk,
                         fprovince,
                         fm_id,
                       round(coalesce(sum(fmoney),0),2) fmoney,
                       coalesce(sum(fpayuser_cnt),0) fpayuser_cnt,
                       coalesce(sum(forder),0) forder,
                       CASE coalesce(sum(forder),0)
                           WHEN 0 THEN 0
                           ELSE coalesce(sum(fpayuser_cnt),0)/coalesce(sum(forder),0)*100
                       END ocr
                FROM analysis.pay_channel_province_fct a
                WHERE fdate >= to_date('%(ld_7dayago)s', 'yyyy-mm-dd')
                  AND fdate <= to_date('%(ld_end)s', 'yyyy-mm-dd')
                  AND a.fgamefsk !=4118194431
                GROUP BY to_char(fdate,'YYYY-MM-DD'),
                         fprovince,
                         fm_id
                  """ % self.sql_dict

        data = self.query(sql)

        #格式化成字典
        for row in data:
            key = (row['fgamefsk'],row['fm_id'],row['fprovince'])
            self.metadata.setdefault(key,{})
            for dim in self.dims:
                self.metadata[key].setdefault(dim,{})
                self.metadata[key][dim][row.get('fdate')] = row.get(dim,0)



    def get_config_data(self):
        #将报警配置表内容取出并格式化成字典
        sql="""SELECT *
                FROM analysis.alarm_pay_config gm
                WHERE onoff=1
                --and gameid = 1396895  AND platid = 58930170  AND verid = 58606689
                """
        cons = self.query(sql)
        for row in cons:
            key = (row['fgamefsk'],row['fm_id'],row['fprovince'])
            self.con_data.setdefault(key,{})
            self.con_data[key].setdefault(row.get('fdim','-'),{})
            self.con_data[key][row.get('fdim','-')] = row


    def get_alarm_data(self):
        tmp_thb = {}
        for k in self.metadata.keys():
            tmp_thb.setdefault(k,{})
            for d in self.dims:

                #求同比，环比
                tmp_thb[k].setdefault(d,{})
                tmp1 = self.metadata.get(k,{}).get(d,{})
                tmp_thb[k][d]= get_tb_hb(tmp1,stat_date)

                #取出此粒度此维度的报警配置条件：
                con = self.con_data.get(k ,{}).get(d,{})
                #如果在配置列表都没设置报警条件，就不用管这条了
                if not con:
                    continue

                #这是报警的一些前提条件配置：
                days_pre = con.get('fdays_pre','5')
                dim_pre = con.get('fdim_pre','fmoney')
                updown_pre = con.get('updown_pre','>=')
                fvalue_pre = con.get('fvalue_pre','100')
                tmp = self.metadata[k][dim_pre]
                #计算n天内dim_pre的均值
                pre_date = (datetime.datetime.strptime(stat_date, '%Y-%m-%d') - datetime.timedelta(days=days_pre)).strftime('%Y-%m-%d')
                _tmp = [v for kk,v in tmp.items() if kk >= pre_date and kk <= stat_date]

                avg_val = round(round(sum(_tmp),2)/len(_tmp),2) if len(_tmp) else 0
                #如果报警前提条件都不满足，也可以继续下一条了
                if not eval(str(avg_val) + updown_pre + str(fvalue_pre)):
                    continue

                #如果触发同环比设定条件之一，则将此记录数据放入insert_data:
                hb_val = tmp_thb[k][d]['hb']
                tb_val = tmp_thb[k][d]['tb']
                if (hb_val != '-' and hb_val<0 and abs(hb_val) >= con.get('fvalue_hb',0)) or (tb_val != '-' and tb_val<0and abs(tb_val) >= con.get('fvalue_tb',0)) :

                    t_list = ['fgamefsk', 'fpay_kind', 'fmobilename', 'fm_id', 'fm_name', 'fcountry', 'fprovince', 'fdim', 'fdays_pre', 'fdim_pre' ]
                    _r = [stat_date] + [con.get(t,'') for t in t_list] + [avg_val, hb_val, tb_val]
                    self.insert_data.append(_r)


    def insert_into_table(self):
        #将需要报警的版本插入表中
        del_sql = """ delete from analysis.alarm_pay_data where fdate = '%s'""" % stat_date
        self.exe_hql(del_sql)
        sql = """ insert into analysis.alarm_pay_data (fdate,fgamefsk, fpay_kind, fmobilename, fm_id, fm_name, fcountry, fprovince, fdim, fdays_pre, fdim_pre, avg_val, hb_val, tb_val)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        self.exemany_hql(sql, self.insert_data)


    def __call__(self):
        self.get_base_data()
        self.get_config_data()
        self.get_alarm_data()
        self.insert_into_table()


if __name__ == "__main__":
    stat_date = get_stat_date()
    a = agg_alarm_pay_by_day(stat_date)
    a()
