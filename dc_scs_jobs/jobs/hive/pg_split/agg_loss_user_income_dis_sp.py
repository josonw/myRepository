#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date


class agg_loss_user_income_dis_sp(BasePGStat):

    """拆分结果表 dcnew.loss_user_income_dis
       分为 dcnew.loss_user_income_dis_general(付费额度分段：<=1USD，1-10USD，10-50USD，50-100USD，100-500USD，500+USD)
    """

    def stat(self):

        sql = """ delete from dcnew.loss_user_income_dis_general where fdate = '%(stat_date)s';

        insert into dcnew.loss_user_income_dis_general
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fchannelcode
               ,case when coalesce(fincome,0) <=1 then '<=1USD'
                     when fincome >1 and fincome <=10 then '1-10USD'
                     when fincome >10 and fincome <=50 then '10-50USD'
                     when fincome >50 and fincome <=100 then '50-100USD'
                     when fincome >100 and fincome <=500 then '100-500USD'
                     when fincome >500 then '500+USD' end fincome
               ,flossdays
               ,sum(fcloss_unum) fcloss_unum
               ,sum(fdloss_unum) fdloss_unum
          FROM dcnew.loss_user_income_dis
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
                 ,case when coalesce(fincome,0) <=1 then '<=1USD'
                       when fincome >1 and fincome <=10 then '1-10USD'
                       when fincome >10 and fincome <=50 then '10-50USD'
                       when fincome >50 and fincome <=100 then '50-100USD'
                       when fincome >100 and fincome <=500 then '100-500USD'
                       when fincome >500 then '500+USD' end;

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_loss_user_income_dis_sp(stat_date)
    a()
