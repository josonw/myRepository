#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_loss_user_reflux_gameparty_dis_sp(BasePGCluster):

    """拆分结果表 dcnew.loss_user_reflux_gameparty_dis
       分为 dcnew.loss_user_reflux_gameparty_dis_general(牌局数分段：1，2，3，4，5...18，19，20，21-30，31-50，51-100，100+)
    """

    def stat(self):

        sql = """ delete from dcnew.loss_user_reflux_gameparty_dis_general where fdate = '%(stat_date)s';

        insert into dcnew.loss_user_reflux_gameparty_dis_general
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fchannelcode
               ,flossdays
               ,case when coalesce(fgameparty,0) <=20 then cast (fgameparty as varchar)
                     when fgameparty >=21 and fgameparty <=30 then '21-30'
                     when fgameparty >=31 and fgameparty <=50 then '31-50'
                     when fgameparty >=51 and fgameparty <=100 then '51-100'
                     when fgameparty >100 then '100+' end fgameparty
               ,sum(floss_unum) floss_unum
               ,sum(freflux_unum ) freflux_unum
          FROM dcnew.loss_user_reflux_gameparty_dis
         where fdate = '%(stat_date)s'
         group by fdate
                 ,fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fsubgamefsk
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,fchannelcode
                 ,flossdays
                 ,case when coalesce(fgameparty,0) <=20 then cast (fgameparty as varchar)
                       when fgameparty >=21 and fgameparty <=30 then '21-30'
                       when fgameparty >=31 and fgameparty <=50 then '31-50'
                       when fgameparty >=51 and fgameparty <=100 then '51-100'
                       when fgameparty >100 then '100+' end;

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_loss_user_reflux_gameparty_dis_sp(stat_date)
    a()
