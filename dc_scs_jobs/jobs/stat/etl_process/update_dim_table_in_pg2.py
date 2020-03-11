#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date


class update_dim_table_in_pg2(BasePGStat):

    def stat(self):
        sql = """
                INSERT INTO analysis.scene_dim (fgamefsk, fname, fdisname)
                SELECT fgamefsk,
                       fpay_scene,
                       fpay_scene
                FROM
                  ( SELECT DISTINCT fgamefsk,
                                    fpay_scene
                   FROM analysis.user_generate_order_fct a
                   WHERE fdate >= date'%(ld_begin)s'
                     AND fdate < date'%(ld_end)s'
                     AND fpay_scene !='其他') a
                WHERE NOT EXISTS
                    ( SELECT 1
                     FROM analysis.scene_dim be
                     WHERE be.fgamefsk = a.fgamefsk
                       AND be.fname = a.fpay_scene );
                commit;
        """% self.sql_dict
        self.exe_hql(sql)

        sql = """INSERT INTO analysis.scene_dim (fgamefsk, fname, fdisname)
                 SELECT DISTINCT a.fgamefsk,
                                    a.bankruptscene,
                                    a.bankruptscene
                 FROM analysis.user_bankrupt_scene_fct a
                 left outer join analysis.scene_dim b
                 on a.fgamefsk=b.fgamefsk
                 and a.bankruptscene=b.fname
                 WHERE a.fdate >= date'%(ld_begin)s'
                 AND a.fdate < date'%(ld_end)s'
                 AND b.fname is null and a.bankruptscene !='未定义';
                 commit;
        """% self.sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":
    stat_date = get_stat_date()
    a = update_dim_table_in_pg2(stat_date)
    a()

