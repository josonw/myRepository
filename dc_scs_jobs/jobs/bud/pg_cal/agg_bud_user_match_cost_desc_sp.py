#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date


class agg_bud_user_match_cost_desc_sp(BasePGStat):

    """拆分结果表 bud_dm.bud_user_match_cost_desc
       分为 bud_dm.bud_user_match_cost_desc_general(fmatch_id = '-21379')
         与 bud_dm.bud_user_match_cost_desc_detail(fmatch_id <> '-21379')
    """

    def stat(self):

        sql = """ delete from bud_dm.bud_user_match_cost_desc_general where fdate = '%(stat_date)s';

        insert into bud_dm.bud_user_match_cost_desc_general
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fparty_type
               ,fpname
               ,fsubname
               ,fgsubname
               ,fmatch_id
               ,fio_type
               ,fact_id
               ,fact_desc
               ,fitem_id
               ,funum
               ,fcnt
               ,fitem_num
               ,fcost
          FROM bud_dm.bud_user_match_cost_desc
         where fdate = '%(stat_date)s'
           and fmatch_id = '-21379';

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

        sql = """ delete from bud_dm.bud_user_match_cost_desc_detail where fdate = '%(stat_date)s';

        insert into bud_dm.bud_user_match_cost_desc_detail
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fparty_type
               ,fpname
               ,fsubname
               ,fgsubname
               ,fmatch_id
               ,fio_type
               ,fact_id
               ,fact_desc
               ,fitem_id
               ,funum
               ,fcnt
               ,fitem_num
               ,fcost
          FROM bud_dm.bud_user_match_cost_desc
         where fdate = '%(stat_date)s'
           and fmatch_id <> '-21379';

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_bud_user_match_cost_desc_sp(stat_date)
    a()
