#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_label_payment(BaseStat):
    def create_tab(self):
        """
        fpay_recency:   付费的时间接近度
        fpay_freq:      付费的频率
        fmoney:         付费总金额
        fscore:         将各个指标进行标准化，在加权求和得到付费能力得分
        frank:          付费能力得分在各bpid里面进行排序，得到排名
        frate:          付费能力得分在各bpid里面排名的百分位数
        """
        hql = """
          create table if not exists stage.user_label_payment
          (
            fdate           date,
            fbpid           varchar(100),
            fuid            int,
            fpay_recency    decimal(30,6),
            fpay_freq       decimal(30,4),
            fmoney          decimal(30,4),
            fscore          decimal(30,4),
            frank           bigint,
            frate           decimal(30,10)
          )
          partitioned by(dt date)
        """
        res = self.hq.exe_sql(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        """ 重要部分，统计内容 """
        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
            drop table if exists stage.user_label_payment_tmp_%(num_begin)s;
            create table stage.user_label_payment_tmp_%(num_begin)s as
            select  fbpid,
                    fuid,
                    1 / (datediff('%(ld_daybegin)s', flast_pay_time) + 1) fpay_recency,
                    fpay_num / (datediff(flast_pay_time, ffirst_pay_time))   fpay_freq,
                    fusd
               from stage.user_info_all
              where dt = '%(ld_daybegin)s'
                and flast_act_date > '%(ld_60dayago)s'
                and fusd > 0
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
            insert overwrite table stage.user_label_payment
            partition(dt='%(ld_daybegin)s')
            select '%(ld_daybegin)s' fdate,
                    fbpid,
                    fuid,
                    fpay_recency,
                    fpay_freq,
                    fusd,
                    fscore,
                    row_number() over(partition by fbpid order by fscore desc) frank,
                    row_number() over(partition by fbpid order by fscore desc) / fcnt frate
              from
              (
                  select  a.fbpid,
                          fuid,
                          fpay_recency ,
                          fpay_freq,
                          fusd,
                          fcnt,
                          0.33 * (fpay_recency_max - fpay_recency) / (fpay_recency_max - fpay_recency_min + 0.000001) +
                          0.33 * (fpay_freq - fpay_freq_min) / (fpay_freq_max - fpay_freq_min + 0.000001) +
                          0.34 * (fusd - fusd_min) / (fusd_max - fusd_min + 0.000001) fscore
                   from stage.user_label_payment_tmp_%(num_begin)s a
                   inner join
                   (
                     select fbpid,
                            min(fpay_recency) fpay_recency_min,
                            max(fpay_recency) fpay_recency_max,
                            min(fpay_freq) fpay_freq_min,
                            max(fpay_freq) fpay_freq_max,
                            min(fusd) fusd_min,
                            max(fusd) fusd_max,
                            count(1) fcnt
                       from stage.user_label_payment_tmp_%(num_begin)s
                      group by fbpid
                    ) b
                    on a.fbpid = b.fbpid
              ) t
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """drop table if exists stage.user_label_payment_tmp_%(num_begin)s""" % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_label_payment(statDate)
    a()
