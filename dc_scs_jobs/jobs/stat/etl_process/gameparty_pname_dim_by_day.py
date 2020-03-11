#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date

class gameparty_pname_dim_by_day(BasePGStat):

    def stat(self):
        sql = """
            DELETE
            FROM analysis.gameparty_pname_dim_by_day a
            WHERE a.fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
              AND a.fdate < to_date('%(ld_end)s', 'yyyy-mm-dd');

            commit;
        """% self.sql_dict
        self.exe_hql(sql)


        sql = """
        INSERT INTO analysis.gameparty_pname_dim_by_day (fdate, fgamefsk, fplatformfsk, fversionfsk, fpname)
        SELECT DISTINCT fdate,
                        fgamefsk,
                        fplatformfsk,
                        fversionfsk,
                        coalesce(a.fpname,'未知') fpname
        FROM ANALYSIS.GAMEPARTY_MUSTBLIND_FCT a
        WHERE a.fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
          AND a.fdate < to_date('%(ld_end)s', 'yyyy-mm-dd')
          union
        SELECT DISTINCT fdate,
                        fgamefsk,
                        fplatformfsk,
                        fversionfsk,
                        coalesce(a.fpname,'未知') fpname
        FROM ANALYSIS.gameparty_settlement_fct a
        WHERE a.fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
          AND a.fdate < to_date('%(ld_end)s', 'yyyy-mm-dd');
        commit;
        """% self.sql_dict
        self.exe_hql(sql)



if __name__ == "__main__":
    stat_date = get_stat_date()
    a = gameparty_pname_dim_by_day(stat_date)
    a()
