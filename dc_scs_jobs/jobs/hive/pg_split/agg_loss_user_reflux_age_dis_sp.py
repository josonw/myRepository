#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date


class agg_loss_user_reflux_age_dis_sp(BasePGStat):

    """拆分结果表 dcnew.loss_user_reflux_age_dis
       分为 dcnew.loss_user_reflux_age_dis_general(年龄分段：<=1，2-3，4-7，8-14，15-30，31-90，91-180，181-365，366+)
    """

    def stat(self):

        sql = """ delete from dcnew.loss_user_reflux_age_dis_general where fdate = '%(stat_date)s';

        insert into dcnew.loss_user_reflux_age_dis_general
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fchannelcode
               ,flossdays
               ,case when nvl(fage,0) <=1 then '<=1'
                     when fage >=2 and fage <=3 then '2-3'
                     when fage >=4 and fage <=7 then '4-7'
                     when fage >=8 and fage <=14 then '8-14'
                     when fage >=15 and fage <=30 then '15-30'
                     when fage >=31 and fage <=90 then '31-90'
                     when fage >=91 and fage <=180 then '91-180'
                     when fage >=181 and fage <=365 then '181-365'
                     when fage >=366 then '366+' end fage
               ,sum(floss_unum) floss_unum
               ,sum(freflux_unum ) freflux_unum
          FROM dcnew.loss_user_reflux_age_dis
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
                 ,case when nvl(fage,0) <=1 then '<=1'
                       when fage >=2 and fage <=3 then '2-3'
                       when fage >=4 and fage <=7 then '4-7'
                       when fage >=8 and fage <=14 then '8-14'
                       when fage >=15 and fage <=30 then '15-30'
                       when fage >=31 and fage <=90 then '31-90'
                       when fage >=91 and fage <=180 then '91-180'
                       when fage >=181 and fage <=365 then '181-365'
                       when fage >=366 then '366+' end;

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_loss_user_reflux_age_dis_sp(stat_date)
    a()
