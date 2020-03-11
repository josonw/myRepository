#-*- coding: UTF-8 -*-
# Author:AnsenWen
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_gameparty_all(BaseStat):
    def create_tab(self):
        """
        建表:             用户玩牌全量表，计算日期从2014-01-01开始
        ffirst_play_date: 首次玩牌时间
        flast_play_date:  最后一次玩牌时间
        fcharge:          总台费
        fplay_inning:     总玩牌局数
        fplay_time:       总玩牌时长
        fplay_day:        总玩牌天数
        fwin_inning:      赢牌总局数
        flose_inning:     输牌总局数
        fwin_gamecoins:   赢得总金币数
        flose_gamecoins:  输掉总金币数
        """
        hql = """
            create table if not exists stage.user_gameparty_all
            (
                fdate           date,
                fbpid           varchar(64),
                fuid            int,
                ffirst_play_date date,
                flast_play_date date,
                fcharge         bigint,
                fplay_inning    bigint,
                fplay_time      bigint,
                fplay_day       int,
                fwin_inning     bigint,
                flose_inning    bigint,
                fwin_gamecoins  bigint,
                flose_gamecoins bigint
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
          insert overwrite table stage.user_gameparty_all
          partition(dt='%(ld_daybegin)s')
          select '%(ld_daybegin)s' fdate,
                 fbpid,
                 fuid,
                 min(ffirst_play_date) ffirst_play_date,
                 max(flast_play_date) flast_play_date,
                 sum(fcharge) fcharge,
                 sum(fplay_inning) fplay_inning,
                 sum(fplay_time) fplay_time,
                 max(fplay_day) + max(is_new) fplay_day,
                 sum(fwin_inning) fwin_inning,
                 sum(flose_inning) flose_inning,
                 sum(fwin_gamecoins) fwin_gamecoins,
                 sum(flose_gamecoins) flose_gamecoins
          from(
                select fbpid,
                       fuid,
                       ffirst_play_date,
                       flast_play_date,
                       fcharge,
                       fplay_inning,
                       fplay_time,
                       fplay_day,
                       fwin_inning,
                       flose_inning,
                       fwin_gamecoins,
                       flose_gamecoins,
                       0 is_new
                  from user_gameparty_all
                 where dt = '%(ld_1dayago)s'
                 union all
                 select fbpid,
                        fuid,
                        fdate ffirst_play_date,
                        fdate flast_play_date,
                        fcharge,
                        fparty_num fplay_inning,
                        fplaytime fplay_time,
                        1 fplay_day,
                        fwin_party_num fwin_inning,
                        flose_party_num flose_inning,
                        fwin_num fwin_gamecoins,
                        flose_num flose_gamecoins,
                        1 is_new
                  from stage.user_gameparty_info_mid
                 where dt = '%(ld_daybegin)s'
          ) t
           group by fbpid, fuid
        """ % query

        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_gameparty_all(statDate)
    a()
