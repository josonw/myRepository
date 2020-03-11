#-*- coding: UTF-8 -*-
# Author: AnsenWen
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_label_activity(BaseStat):
    def create_tab(self):
        """
        flogin_recency: 登录的时间接近度
        flogin_freq:    登录的频率
        flogin_num:     登录的次数
        fonline_time:   在线时长，单位：秒
        fscore:         将各个指标进行标准化，在加权求和得到活跃度得分
        frank:          活跃度得分在各bpid里面进行排序，得到排名
        frate:          活跃度得分在各bpid里面排名的百分位数
        """
        hql = """
          create table if not exists stage.user_label_activity
          (
            fdate           date,
            fbpid           varchar(100),
            fuid            int,
            flogin_recency  decimal(30,4),
            flogin_freq     decimal(30,4),
            flogin_num      int,
            fonline_time    int,
            fscore          decimal(30, 4),
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
        #调试的时候把调试开关打开，正式执行的时候设置为0
        #self.hq.debug = 0
        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
            drop table if exists stage.user_label_activity_tmp_%(num_begin)s;
            create table stage.user_label_activity_tmp_%(num_begin)s as
            select fbpid,
                    fuid,
                    1 / (datediff('%(ld_daybegin)s', flast_log_date) + 1) flogin_recency,
                    flogin_day / (datediff(flast_log_date, ffirst_log_date) + 1) flogin_freq,
                    flogin_num,
                    fonline_time
               from stage.user_info_all
              where dt = '%(ld_daybegin)s'
                and flast_act_date > '%(ld_60dayago)s'
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
            insert overwrite table stage.user_label_activity
            partition(dt='%(ld_daybegin)s')
            select '%(ld_daybegin)s' fdate,
                    fbpid,
                    fuid,
                    flogin_recency,
                    flogin_freq,
                    flogin_num,
                    fonline_time,
                    fscore,
                    row_number() over(partition by fbpid order by fscore desc) frank,
                    row_number() over(partition by fbpid order by fscore desc) / fcnt frate
              from
              (
                  select  a.fbpid,
                          fuid,
                          flogin_recency,
                          flogin_freq,
                          flogin_num,
                          fonline_time,
                          fcnt,
                          0.25 * (flogin_recency_max - flogin_recency) / (flogin_recency_max - flogin_recency_min) +
                          0.25 * (flogin_freq - flogin_freq_min) / (flogin_freq_max - flogin_freq_min) +
                          0.25 * (flogin_num - flogin_num_min) / (flogin_num_max - flogin_num_min) +
                          0.25 * (fonline_time - fonline_time_min) / (fonline_time_max - fonline_time_min) fscore
                   from stage.user_label_activity_tmp_%(num_begin)s a
                   join
                   (
                     select fbpid,
                            min(flogin_recency) flogin_recency_min,
                            max(flogin_recency) flogin_recency_max,
                            min(flogin_freq) flogin_freq_min,
                            max(flogin_freq) flogin_freq_max,
                            min(flogin_num) flogin_num_min,
                            max(flogin_num) flogin_num_max,
                            min(fonline_time) fonline_time_min,
                            max(fonline_time) fonline_time_max,
                            count(1) fcnt
                       from stage.user_label_activity_tmp_%(num_begin)s
                      group by fbpid
                    ) b
                    on a.fbpid = b.fbpid
              ) t
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """drop table if exists stage.user_label_activity_tmp_%(num_begin)s""" % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_label_activity(statDate)
    a()
