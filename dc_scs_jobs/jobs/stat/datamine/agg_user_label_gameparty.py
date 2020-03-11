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

class agg_user_label_gameparty(BaseStat):
    def create_tab(self):
        """
        fplay_freq:     玩牌频率
        fplay_day:      玩牌天数
        fplay_inning:   玩牌局数
        fmax_pname:     玩牌玩得最多的场次（大厅模式下便是子游戏）
        fscore:         将各个指标进行标准化，在加权求和得到玩牌活跃度得分
        frank:          玩牌活跃度得分在各bpid里面进行排序，得到排名
        frate:          玩牌活跃度得分在各bpid里面排名的百分位数
        """
        hql = """
          create table if not exists stage.user_label_gameparty
          (
            fdate           date,
            fbpid           varchar(100),
            fuid            int,
            fplay_freq      decimal(30,4),
            fplay_day       int,
            fplay_inning    int,
            fmax_pname      varchar(32),
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
            drop table if exists stage.user_label_gameparty_tmp_%(num_begin)s;
            create table stage.user_label_gameparty_tmp_%(num_begin)s as
            select  fbpid,
                    fuid,
                    fplay_day / (datediff(flast_play_date, ffirst_play_date) + 1) fplay_freq,
                    fplay_inning,
                    fplay_time,
                    fmax_pname
               from stage.user_info_all
              where dt = '%(ld_daybegin)s'
                and flast_act_date > '%(ld_60dayago)s'
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        # 活跃度得分以及在各bpid里面的排名
        hql = """
            insert overwrite table stage.user_label_gameparty
            partition(dt='%(ld_daybegin)s')
            select '%(ld_daybegin)s' fdate,
                    fbpid,
                    fuid,
                    fplay_freq,
                    fplay_inning,
                    fplay_time,
                    fmax_pname,
                    fscore,
                    row_number() over(partition by fbpid order by fscore desc) frank,
                    row_number() over(partition by fbpid order by fscore desc) / fcnt frate
              from
              (
                  select  a.fbpid,
                          fuid,
                          fplay_freq,
                          fplay_inning,
                          fplay_time,
                          fmax_pname,
                          fcnt,
                          0.33 * (fplay_freq - fplay_freq_min) / (fplay_freq_max - fplay_freq_min) +
                          0.33 * (fplay_inning - fplay_inning_min) / (fplay_inning_max - fplay_inning_min) +
                          0.34 * (fplay_time - fplay_time_min) / (fplay_time_max - fplay_time_min) fscore
                   from stage.user_label_gameparty_tmp_%(num_begin)s a
                   join
                   (
                     select fbpid,
                            min(fplay_freq) fplay_freq_min,
                            max(fplay_freq) fplay_freq_max,
                            min(fplay_inning) fplay_inning_min,
                            max(fplay_inning) fplay_inning_max,
                            min(fplay_time) fplay_time_min,
                            max(fplay_time) fplay_time_max,
                            count(1) fcnt
                       from stage.user_label_gameparty_tmp_%(num_begin)s
                      group by fbpid
                    ) b
                    on a.fbpid = b.fbpid
              ) t
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """drop table if exists stage.user_label_gameparty_tmp_%(num_begin)s""" % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_label_gameparty(statDate)
    a()
