#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_gameparty_pname_property_dis_sp(BasePGCluster):

    """拆分结果表 dcnew.gameparty_pname_property_dis
       分为 dcnew.gameparty_pname_property_dis_detail(pname <> '-21379')
         与 dcnew.gameparty_pname_property_dis_general(pname = '-21379')
    """

    def stat(self):

        sql = """ delete from dcnew.gameparty_pname_property_dis_general where fdate = '%(stat_date)s';

        insert into dcnew.gameparty_pname_property_dis_general
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fchannelcode
               ,fnum
               ,fname
               ,fpartyname
               ,fusernum
               ,fpname
          FROM dcnew.gameparty_pname_property_dis
         where fdate = '%(stat_date)s'
           and fpname = '-21379';

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

        sql = """ delete from dcnew.gameparty_pname_property_dis_detail where fdate = '%(stat_date)s';

        insert into dcnew.gameparty_pname_property_dis_detail
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fchannelcode
               ,fnum
               ,fname
               ,fpartyname
               ,fusernum
               ,fpname
          FROM dcnew.gameparty_pname_property_dis
         where fdate = '%(stat_date)s'
           and fpname <> '-21379';

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_gameparty_pname_property_dis_sp(stat_date)
    a()
