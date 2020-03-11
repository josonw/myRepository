#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_label_win(BaseStat):
    def create_tab(self):
        """
        fwin_gamecoins: 赢得金币
        fwin_inning:    赢牌局数
        fwin_rate:      胜率
        fmax_pname:     玩牌玩得最多的场次（大厅模式下便是子游戏）
        fscore:         将各个指标进行标准化，在加权求和得到玩牌活跃度得分
        frank:          玩牌活跃度得分在各bpid里面进行排序，得到排名
        frate:          玩牌活跃度得分在各bpid里面排名的百分位数
        """
        hql = """
          create table if not exists stage.user_label_win
          (
            fdate           date,
            fbpid           varchar(100),
            fuid            int,
            fwin_gamecoins  bigint,
            fwin_inning     int,
            fwin_rate       decimal(30, 4),
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
        dates_dict = PublicFunc.date_define(self.stat_date)
        query = {}
        query.update(dates_dict)

        res = self.hq.exe_sql("""use stage; set hive.exec.dynamic.partition.mode=nonstrict; """)
        if res != 0: return res

        hql = """
            drop table if exists stage.user_label_win_tmp_%(num_begin)s;
            create table stage.user_label_win_tmp_%(num_begin)s as
            select fbpid,
                    fuid,
                    fwin_gamecoins,
                    fwin_inning,
                    fwin_inning / fplay_inning fwin_rate,
                    fmax_pname
               from stage.user_info_all
              where dt = '%(ld_daybegin)s'
                and flast_act_date > '%(ld_60dayago)s'
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """
            insert overwrite table stage.user_label_win
            partition(dt='%(ld_daybegin)s')
            select '%(ld_daybegin)s' fdate,
                    fbpid,
                    fuid,
                    fwin_gamecoins,
                    fwin_inning,
                    fwin_rate,
                    fmax_pname,
                    fscore,
                    row_number() over(partition by fbpid order by fscore desc) frank,
                    row_number() over(partition by fbpid order by fscore desc) / fcnt frate
              from
              (
                  select  a.fbpid,
                          fuid,
                          fwin_gamecoins,
                          fwin_inning,
                          fwin_rate,
                          fcnt,
                          fmax_pname,
                          0.33 * (fwin_inning - fwin_inning_min) / (fwin_inning_max - fwin_inning_min) +
                          0.33 * (fwin_gamecoins - fwin_gamecoins_min) / (fwin_gamecoins_max - fwin_gamecoins_min) +
                          0.34 * (fwin_rate - fwin_rate_min) / (fwin_rate_max - fwin_rate_min) fscore
                   from stage.user_label_win_tmp_%(num_begin)s a
                   inner join
                   (
                     select fbpid,
                            min(fwin_inning) fwin_inning_min,
                            max(fwin_inning) fwin_inning_max,
                            min(fwin_gamecoins) fwin_gamecoins_min,
                            max(fwin_gamecoins) fwin_gamecoins_max,
                            min(fwin_rate) fwin_rate_min,
                            max(fwin_rate) fwin_rate_max,
                            count(1) fcnt
                       from stage.user_label_win_tmp_%(num_begin)s
                      group by fbpid
                    ) b
                    on a.fbpid = b.fbpid
              ) t
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        hql = """drop table if exists stage.user_label_win_tmp_%(num_begin)s""" % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res
        return res

if __name__ == '__main__':
  #愉快的统计要开始啦
  statDate = get_stat_date()
  #生成统计实例
  a = agg_user_label_win(statDate)
  a()
