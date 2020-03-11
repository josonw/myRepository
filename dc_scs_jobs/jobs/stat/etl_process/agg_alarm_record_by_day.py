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
from BaseStat import BasePGStat, get_stat_date, pg_db

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
    result = {'环比':{},'同比':{}}
    d = round(data.get(date, 0), 2) if data.get(date, 0) % 1 else data.get(date, 0)
    if type == 'd':
        hb_day_data = data.get(PublicFunc.add_days(date, -1), 0)
        tb_day_data = data.get(PublicFunc.add_days(date, -7), 0)
    elif type == 'm':
        hb_day_data = data.get(PublicFunc.add_months(date, -1), 0)
        tb_day_data = data.get(PublicFunc.add_months(date, -3), 0)
    hb_per = get_growth_rate(d, hb_day_data)
    tb_per = get_growth_rate(d, tb_day_data)
    result['环比'].setdefault('升高' if hb_per>0 else '降低',hb_per)
    result['同比'].setdefault('升高' if tb_per>0 else '降低',tb_per)

    return result


class agg_alarm_record_by_day(BasePGStat):
    def __init__(self, stat_date=datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), "%Y-%m-%d"), eid = 0):
        BasePGStat.__init__(self, stat_date)
        self.stat_date = stat_date
        self.ld_1dayago = PublicFunc.add_days(stat_date, -1)
        self.ld_7dayago = PublicFunc.add_days(stat_date, -7)
        self.ld_month_begin = PublicFunc.trunc(stat_date, "MM")
        self.ld_1month_ago_begin = PublicFunc.add_months(self.ld_month_begin, -1)
        self.last_day = PublicFunc.last_day(stat_date)
        self.dims_day = ['dsu', 'dau', 'pun', 'dpu', 'dip']
        self.dims_month = ['mau', 'msu', 'mip']
        self.sql_dict = {
            'stat_date':  self.stat_date,
            'ld_1dayago': self.ld_1dayago,
            'ld_7dayago': self.ld_7dayago,
            'ld_month_begin': self.ld_month_begin,
            'ld_1month_ago_begin': self.ld_1month_ago_begin,
            'ld_3month_ago_begin': PublicFunc.add_months(self.ld_month_begin, -3),
            }
        self.data_day = {}
        self.data_month = {}
        self.datam =[]
        self.con_data = {}
        self.insert_data = []
        self.en_cn_dim = {  'dau':u'日活跃用户数',
                            'dsu':u'日新增用户数',
                            'pun':u'日玩牌用户数',
                            'dpu':u'日付费用户数',
                            'dip':u'日付费额',
                            'mau':u'月活跃用户数',
                            'msu':u'月新增用户数',
                            'mip':u'月付费额',}

        self.en_cn_flag = { 1:'游戏预警通知',
                            2:'平台预警通知',
                            3:'版本预警通知'}



    def get_base_data_month(self):
        sql = """ SELECT to_char(fdate,'yyyy-mm-dd') fdate,
                           fgamefsk,
                           fplatformfsk,
                           fversionfsk,
                           coalesce(mau,0) mau,
                           coalesce(msu,0) msu,
                           coalesce(mip,0) mip
                    FROM summary_data_by_month_fct
                    WHERE fdate IN (date'%(ld_month_begin)s',date'%(ld_1month_ago_begin)s',date'%(ld_3month_ago_begin)s')
                     --and FGAMEFSK = 1396895  AND FPLATFORMFSK = 58930170  AND FVERSIONFSK = 58606689
                    UNION
                    SELECT to_char(fdate,'yyyy-mm-dd') fdate,
                           fgamefsk,
                           fplatformfsk,
                           0 fversionfsk,
                             coalesce(sum(mau),0) mau,
                             coalesce(sum(msu),0) msu,
                             coalesce(sum(mip),0) mip
                    FROM summary_data_by_month_fct
                    WHERE fdate IN (date'%(ld_month_begin)s',date'%(ld_1month_ago_begin)s',date'%(ld_3month_ago_begin)s')
                     --and FGAMEFSK = 1396895  AND FPLATFORMFSK = 58930170  AND FVERSIONFSK = 58606689
                    GROUP BY to_char(fdate,'yyyy-mm-dd') ,
                             fgamefsk ,
                             fplatformfsk
                    UNION
                    SELECT  to_char(fdate,'yyyy-mm-dd') fdate,
                            fgamefsk,
                            0 fplatformfsk,
                            0 fversionfsk,
                            coalesce(sum(mau),0) mau,
                            coalesce(sum(msu),0) msu,
                            coalesce(sum(mip),0) mip
                    FROM summary_data_by_month_fct
                    WHERE fdate IN (date'%(ld_month_begin)s',date'%(ld_1month_ago_begin)s',date'%(ld_3month_ago_begin)s')
                     --and FGAMEFSK = 1396895  AND FPLATFORMFSK = 58930170  AND FVERSIONFSK = 58606689
                    GROUP BY to_char(fdate,'yyyy-mm-dd') ,
                             fgamefsk """ % self.sql_dict

        self.datam = self.query(sql)


    def get_base_data_day(self):
        sql = """SELECT to_char(fdate,'yyyy-mm-dd') fdate,
                       fgamefsk,
                       fplatformfsk,
                       fversionfsk,
                       coalesce(dau,0) dau,
                       coalesce(dsu,0) dsu,
                       coalesce(pun,0) pun,
                       coalesce(dpu,0) dpu,
                       coalesce(dip,0) dip
                FROM analysis.summary_data_by_day_fct
                WHERE fdate IN (date'%(stat_date)s',date'%(ld_1dayago)s',date'%(ld_7dayago)s')
                 --and FGAMEFSK = 1396895  AND FPLATFORMFSK = 58930170  AND FVERSIONFSK = 58606689
                UNION
                SELECT to_char(fdate,'yyyy-mm-dd') fdate,
                       fgamefsk,
                       fplatformfsk,
                       0 fversionfsk,
                         coalesce(sum(dau),0) dau,
                         coalesce(sum(dsu),0) dsu,
                         coalesce(sum(pun),0) pun,
                         coalesce(sum(dpu),0) dpu,
                         coalesce(sum(dip),0) dip
                FROM analysis.summary_data_by_day_fct
                WHERE fdate IN (date'%(stat_date)s',date'%(ld_1dayago)s',date'%(ld_7dayago)s')
                 --and FGAMEFSK = 1396895  AND FPLATFORMFSK = 58930170  AND FVERSIONFSK = 58606689
                GROUP BY to_char(fdate,'yyyy-mm-dd'),
                         fgamefsk,
                         fplatformfsk
                UNION
                SELECT to_char(fdate,'yyyy-mm-dd') fdate,
                       fgamefsk,
                        0 fplatformfsk,
                        0 fversionfsk,
                         coalesce(sum(dau),0) dau,
                         coalesce(sum(dsu),0) dsu,
                         coalesce(sum(pun),0) pun,
                         coalesce(sum(dpu),0) dpu,
                         coalesce(sum(dip),0) dip
                FROM analysis.summary_data_by_day_fct
                WHERE fdate IN (date'%(stat_date)s',date'%(ld_1dayago)s',date'%(ld_7dayago)s')

                 --and FGAMEFSK = 1396895  AND FPLATFORMFSK = 58930170  AND FVERSIONFSK = 58606689
                GROUP BY to_char(fdate,'yyyy-mm-dd'),
                         fgamefsk
                  """ % self.sql_dict

        data = self.query(sql)
        tmp_day = {}
        #如果为每月最后一天，将月数据指标加进来
        if self.stat_date == self.last_day:
            all_data = self.datam + data

            dims = self.dims_day + self.dims_month
        else:
            all_data = data
            dims = self.dims_day

        #格式化成字典
        for row in all_data:
            gpv = (row.get('fgamefsk','-'),row.get('fplatformfsk','-'),row.get('fversionfsk','-'))
            tmp_day.setdefault(gpv,{})
            for dim in dims:
                tmp_day[gpv].setdefault(dim,{})
                tmp_day[gpv][dim][row.get('fdate')] = row.get(dim,0)

        #求同比，环比
        for k in tmp_day.keys():
            for d in dims:
                tmp = tmp_day.get(k,{}).get(d,{})
                if d in ('msu','mau','mip'):
                    tmp_day[k][d]= get_tb_hb(tmp,self.ld_month_begin,'m')
                else:
                    tmp_day[k][d]= get_tb_hb(tmp,stat_date)
        self.data_day = tmp_day
        #print '**************',tmp_day

    def get_config_data(self):
        #将报警配置表内容取出并格式化成字典
        sql="""SELECT gameid,
                       platid,
                       verid,
                       dim,
                       thbi,
                       updown,
                       fvalue,
                       flag,
                       fcreater
                FROM analysis.user_alarm_config gm
                WHERE onoff=1
                --and gameid = 1396895  AND platid = 58930170  AND verid = 58606689
                """
        cons = self.query(sql)
        for row in cons:
            gpv = (row.get('gameid','-'),row.get('platid','-'),row.get('verid','-'))
            self.con_data.setdefault(gpv,{})
            self.con_data[gpv].setdefault(row.get('dim','-'),{})
            self.con_data[gpv][row.get('dim','-')].setdefault(row.get('thbi','-'),{})
            self.con_data[gpv][row.get('dim','-')][row.get('thbi','-')].setdefault(row.get('updown','-'),{})
            self.con_data[gpv][row.get('dim','-')][row.get('thbi','-')][row.get('updown','-')] ={'value':row.get('fvalue',0),'flag':row.get('flag',3),'fcreater':row.get('fcreater','-')}
        #print '**************',self.con_data

    def get_alarm_data(self):
        #对报警配置的每一条去查找当天的数据，若能找到且同环比超过预警值则存入列表，以待插入表中
        for k1 in self.con_data.keys():
            tmp1 = self.con_data[k1]
            for k2 in tmp1.keys():
                tmp2 = tmp1[k2]
                for k3 in tmp2.keys():
                    tmp3= tmp2[k3]
                    for k4 in tmp3.keys():
                        dd = self.data_day.get(k1,{}).get(k2,{}).get(k3,{}).get(k4,'*')
                        if  dd == '*':
                            continue
                        else:
                            if abs(dd)>tmp3[k4].get('value',0):
                                #print '#########',abs(dd),tmp3[k4].get('value',0)
                                flag = tmp3[k4].get('flag',0)
                                fcreater = tmp3[k4].get('fcreater','-')
                                _r = [self.stat_date,k1[0],k1[1],k1[2],k2,self.en_cn_dim.get(k2,'-'),k3,k4,dd,flag,self.en_cn_flag.get(flag,'-'),fcreater]
                                self.insert_data.append(_r)
        #print '&&&&&&&&&',self.insert_data


    def insert_into_table(self):
        #将需要报警的版本插入表中
        del_sql = """ delete from analysis.alarm_data where fdate = '%s'""" % stat_date
        self.exe_hql(del_sql)
        sql = """ insert into analysis.alarm_data (fdate, fgamefsk, fplatformfsk, fversionfsk, dim, dim_cn, thbi, updown, fpercent, flag, flag_cn, fcreater)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        self.exemany_hql(sql, self.insert_data)


    def __call__(self):
        self.get_base_data_month()
        self.get_base_data_day()
        self.get_config_data()
        self.get_alarm_data()
        self.insert_into_table()


if __name__ == "__main__":
    stat_date = get_stat_date()
    a = agg_alarm_record_by_day(stat_date)
    a()