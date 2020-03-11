#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date


class agg_bud_click_event_info_sp(BasePGStat):

    """拆分结果表 bud_dm.bud_click_event_info
       分为 bud_dm.bud_click_event_info_general(fgsubname = '-21379')
         与 bud_dm.bud_click_event_info_detail(fgsubname <> '-21379')
    """

    def stat(self):

        sql = """ delete from bud_dm.bud_click_event_info_general where fdate = '%(stat_date)s';

        insert into bud_dm.bud_click_event_info_general
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fpname
               ,fsubname
               ,fgsubname
               ,fact_id
               ,funum
               ,fnum
          FROM bud_dm.bud_click_event_info
         where fdate = '%(stat_date)s'
           and fgsubname = '-21379';

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

        sql = """ delete from bud_dm.bud_click_event_info_detail where fdate = '%(stat_date)s';

        insert into bud_dm.bud_click_event_info_detail
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fpname
               ,fsubname
               ,fgsubname
               ,fact_id
               ,funum
               ,fnum
          FROM bud_dm.bud_click_event_info
         where fdate = '%(stat_date)s'
           and fgsubname <> '-21379';

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_bud_click_event_info_sp(stat_date)
    a()
