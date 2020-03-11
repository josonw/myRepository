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

class agg_user_gameparty_pname_all(BaseStat):
    def create_tab(self):
        """
        建表:                用户分牌局场次统计的全量表
        fpname:              场次名称
        fparty_num:          牌局次数
        fcharge:             台费
        fwin_gc_num:         赢得金币数
        flose_gc_num:        输掉金币数
        fwin_party_num:      赢牌局数
        flose_party_num：    输牌局数
        fplaytime：          玩牌时长
        flast_play_date:     最后玩牌的日期
        """
        hql = """
          create table if not exists stage.user_gameparty_pname_all
          (
                fdate               date,
                fbpid               varchar(100),
                fuid                bigint,
                fpname              varchar(100),
                fparty_num          int,
                fcharge             int,
                fwin_gc_num         bigint,
                flose_gc_num        bigint,
                fwin_party_num      int,
                flose_party_num     int,
                fplaytime           int,
                flast_play_date     date
          )
          partitioned by(dt date)
        """
        res = self.hq.exe_sql(hql)
        if res != 0: return res

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
        insert overwrite table stage.user_gameparty_pname_all
        partition(dt='%(ld_daybegin)s')
        select '%(ld_daybegin)s'    fdate,
               fbpid,
               fuid,
               fpname,
               sum(fparty_num)      fparty_num,
               sum(fcharge)         fcharge,
               sum(fwin_gc_num)     fwin_gc_num,
               sum(flose_gc_num)    flose_gc_num,
               sum(fwin_party_num)  fwin_party_num,
               sum(flose_party_num) flose_party_num,
               sum(fplaytime)       fplaytime,
               max(flast_play_date) flast_play_date
          from (select fbpid,
                       fuid,
                       fpname,
                       fparty_num,
                       fcharge,
                       fwin_gc_num,
                       flose_gc_num,
                       fwin_party_num,
                       flose_party_num,
                       fplaytime,
                       flast_play_date
                  from stage.user_gameparty_pname_all
                 where dt = '%(ld_1dayago)s'

                union all

                select fbpid,
                       fuid,
                       fpname,
                       sum(fparty_num)      fparty_num,
                       sum(fcharge)         fcharge,
                       sum(fwin_num)        fwin_gc_num,
                       sum(flose_num)       flose_gc_num,
                       sum(fwin_party_num)  fwin_party_num,
                       sum(flose_party_num) flose_party_num,
                       sum(fplaytime)       fplaytime,
                       max(fdate)           flast_play_date
                  from stage.user_gameparty_info_mid
                 where dt = '%(ld_daybegin)s'
                 group by fbpid, fuid, fpname) t
         group by fbpid, fuid, fpname
        """ % query
        res = self.hq.exe_sql(hql)
        if res != 0: return res

        return res

if __name__ == '__main__':
    #愉快的统计要开始啦
    statDate = get_stat_date()
    #生成统计实例
    a = agg_user_gameparty_pname_all(statDate)
    a()