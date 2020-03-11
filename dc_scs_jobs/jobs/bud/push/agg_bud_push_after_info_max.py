#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date


class agg_bud_push_after_info_max(BasePGStat):

    """推送结果汇总
    """

    def stat(self):

        sql = """ drop table if exists bud_dm.bud_push_after_info_tmp;

        create table bud_dm.bud_push_after_info_tmp as select * from bud_dm.bud_push_after_info t;

        delete from bud_dm.bud_push_after_info;

        insert into bud_dm.bud_push_after_info
        SELECT  fdate
                ,fpush_id
                ,max(act_unum_day1) act_unum_day1
                ,max(act_unum_day2) act_unum_day2
                ,max(act_unum_day3) act_unum_day3
                ,max(act_unum_day4) act_unum_day4
                ,max(act_unum_day5) act_unum_day5
                ,max(act_unum_day6) act_unum_day6
                ,max(act_unum_day7) act_unum_day7
                ,max(play_unum_day1) play_unum_day1
                ,max(play_unum_day2) play_unum_day2
                ,max(play_unum_day3) play_unum_day3
                ,max(play_unum_day4) play_unum_day4
                ,max(play_unum_day5) play_unum_day5
                ,max(play_unum_day6) play_unum_day6
                ,max(play_unum_day7) play_unum_day7
                ,max(act_unum_all) act_unum_all
                ,max(play_unum_all) play_unum_all
          FROM bud_dm.bud_push_after_info_tmp t
         group by fdate ,fpush_id;

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

        sql = """ drop table if exists bud_dm.bud_push_after_gameparty_detail_tmp;

        create table bud_dm.bud_push_after_gameparty_detail_tmp as select * from bud_dm.bud_push_after_gameparty_detail t;

        delete from bud_dm.bud_push_after_gameparty_detail;

        insert into bud_dm.bud_push_after_gameparty_detail
        SELECT  fdate
               ,fgamefsk
               ,fpush_id
               ,fsubname
               ,max(play_unum_day1) play_unum_day1
               ,max(play_unum_day2) play_unum_day2
               ,max(play_unum_day3) play_unum_day3
               ,max(play_unum_day4) play_unum_day4
               ,max(play_unum_day5) play_unum_day5
               ,max(play_unum_day6) play_unum_day6
               ,max(play_unum_day7) play_unum_day7
               ,max(partynum_day1) partynum_day1
               ,max(partynum_day2) partynum_day2
               ,max(partynum_day3) partynum_day3
               ,max(partynum_day4) partynum_day4
               ,max(partynum_day5) partynum_day5
               ,max(partynum_day6) partynum_day6
               ,max(partynum_day7) partynum_day7
          FROM bud_dm.bud_push_after_gameparty_detail_tmp t
         group by fdate
               ,fgamefsk
               ,fpush_id
               ,fsubname;

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_bud_push_after_info_max(stat_date)
    a()
