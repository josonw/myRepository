#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BaseStat, get_stat_date

class agg_user_gameparty_week_mid(BaseStat):
    def create_tab(self):
        """
        fplay_inning：       玩牌局数
        fplay_time：         玩牌时间
        fplay_day：          玩牌天数
        fwin_inning：        赢牌局数
        flose_inning：       输牌局数
        fwin_gamecoins：     赢得金币
        flose_gamecoins：    输牌金币
        ffirst_play_time：   首次玩牌时间
        flast_play_time：    最后一次玩牌时间
        """
        hql = """
          use tmp;
          create table if not exists stage.user_gameparty_week_mid
          (
            fdate               date,
            fbpid               varchar(64),
            fuid                int,
            fplay_inning        int,
            fplay_time          bigint,
            fplay_day           int,
            fwin_inning         bigint,
            flose_inning        bigint,
            fwin_gamecoins      bigint,
            flose_gamecoins     bigint,
            ffirst_play_time    string,
            flast_play_time     string
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
          insert overwrite table stage.user_gameparty_week_mid
          partition(dt='%(ld_daybegin)s')
          select '%(ld_daybegin)s'      fdate,
                 fbpid,
                 fuid,
                 sum(fparty_num)        fplay_inning,
                 sum(fplaytime)         fplay_time,
                 count(distinct dt)     fplay_day,
                 sum(fwin_party_num)    fwin_inning,
                 sum(flose_party_num)   flose_inning,
                 sum(fwin_num)          fwin_gamecoins,
                 sum(flose_num)         flose_gamecoins,
                 min(dt)                ffirst_play_time,
                 max(dt)                flast_play_time
            from stage.user_gameparty_info_mid
           where dt >= '%(ld_6dayago)s'
             and dt < '%(ld_dayend)s'
           group by fbpid, fuid
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_gameparty_week_mid(statDate)
    a()
