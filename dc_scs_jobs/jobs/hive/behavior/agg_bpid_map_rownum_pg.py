#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date


class agg_bpid_map_rownum_pg(BasePGStat):

#取每个bpid的周活跃用户数，作为bpid顺序依据
#每周日跑，周日到下周六为一周期
#

    def stat(self):

        sql = """
           delete from dcnew.bpid_map_rownum;
           insert into dcnew.bpid_map_rownum
           select b.fbpid, coalesce(a.f7dactucnt ,0) dau
             from dcbase.bpid_map b
             left join dcnew.act_user a
               on a.fgamefsk = b.fgamefsk
              and a.fplatformfsk = b.fplatformfsk
              and case when a.fhallfsk=-21379 then -13658 else a.fhallfsk end = b.fhallfsk
              and a.fterminaltypefsk = b.fterminaltypefsk
              and a.fversionfsk = b.fversionfsk
              and a.fdate = date'%(stat_date)s'
              and a.fsubgamefsk = -21379
              and a.fchannelcode = -21379;

            update dcbase.bpid_map set fdau = num.dau,priority = num.dau
              from dcnew.bpid_map_rownum num
             where bpid_map.fbpid = num.fbpid;

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_bpid_map_rownum_pg(stat_date)
    a()
