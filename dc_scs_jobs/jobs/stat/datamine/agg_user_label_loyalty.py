#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_label_loyalty(BaseStat):
    def create_tab(self):
        """
        fact_dura:      活跃持续天数，单位：天
        fonline_time:   在线时长，单位：秒
        fplay_time:     游戏时长，单位：秒
        fscore:         将各个指标进行标准化，在加权求和得到忠诚度（用户黏性）得分
        frank:          忠诚度（用户黏性）得分在各bpid里面进行排序，得到排名
        frate:          忠诚度（用户黏性）得分在各bpid里面排名的百分位数
        """
        hql = """
          create table if not exists stage.user_label_loyalty
          (
            fdate           date,
            fbpid           varchar(100),
            fuid            int,
            fact_dura       int,
            fonline_time    int,
            fplay_time      int,
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
            drop table if exists stage.user_label_loyalty_tmp_%(num_begin)s;
            create table stage.user_label_loyalty_tmp_%(num_begin)s as
            select  fbpid,
                    fuid,
                    datediff(flast_act_date, ffirst_act_date) fact_dura,
                    fonline_time,
                    fplay_time
               from stage.user_info_all
              where dt = '%(ld_daybegin)s'
                and flast_act_date > '%(ld_60dayago)s'
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
            insert overwrite table stage.user_label_loyalty
            partition(dt='%(ld_daybegin)s')
            select '%(ld_daybegin)s' fdate,
                    fbpid,
                    fuid,
                    fact_dura ,
                    fonline_time,
                    fplay_time,
                    fscore,
                    row_number() over(partition by fbpid order by fscore desc) frank,
                    row_number() over(partition by fbpid order by fscore desc) / fcnt frate
              from
              (
                  select  a.fbpid,
                          fuid,
                          fact_dura ,
                          fonline_time,
                          fplay_time,
                          fcnt,
                          0.34 * (fact_dura_max - fact_dura) / (fact_dura_max - fact_dura_min) +
                          0.33 * (fonline_time - fonline_time_min) / (fonline_time_max - fonline_time_min) +
                          0.33 * (fplay_time - fplay_time_min) / (fplay_time_max - fplay_time_min) fscore
                   from stage.user_label_loyalty_tmp_%(num_begin)s a
                   inner join
                   (
                     select fbpid,
                            min(fact_dura) fact_dura_min,
                            max(fact_dura) fact_dura_max,
                            min(fonline_time) fonline_time_min,
                            max(fonline_time) fonline_time_max,
                            min(fplay_time) fplay_time_min,
                            max(fplay_time) fplay_time_max,
                            count(1) fcnt
                       from stage.user_label_loyalty_tmp_%(num_begin)s
                      group by fbpid
                    ) b
                    on a.fbpid = b.fbpid
              ) t
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """drop table if exists stage.user_label_loyalty_tmp_%(num_begin)s""" % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_label_loyalty(statDate)
    a()
