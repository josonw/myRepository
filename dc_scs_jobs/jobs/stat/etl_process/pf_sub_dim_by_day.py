#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date

class pf_sub_dim_by_day(BasePGStat):

    def stat(self):
        sql = """
            DELETE
            FROM analysis.pf_sub_dim_by_day a
            WHERE a.fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
              AND a.fdate <= to_date('%(ld_end)s', 'yyyy-mm-dd');

            commit;
        """% self.sql_dict
        self.exe_hql(sql)


        sql = """
        INSERT INTO analysis.pf_sub_dim_by_day (fdate, fgamefsk, fplatformfsk, fversionfsk, fptype,fact_type)
        SELECT DISTINCT fdate,
                        fgamefsk,
                        fplatformfsk,
                        fversionfsk,
                        fptype,
                        fact_type
        FROM ANALYSIS.game_performance_fct a
        WHERE a.fdate >= to_date('%(ld_begin)s', 'yyyy-mm-dd')
          AND a.fdate <= to_date('%(ld_end)s', 'yyyy-mm-dd');
        commit;
        """% self.sql_dict
        self.exe_hql(sql)



if __name__ == "__main__":
    stat_date = get_stat_date()
    a = pf_sub_dim_by_day(stat_date)
    a()
