#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_loss_user_paycnt_dis_sp(BasePGCluster):

    """拆分结果表 dcnew.loss_user_paycnt_dis
       分为 dcnew.loss_user_paycnt_dis_general(付费次数分段：1，2，3-5，6-10，11-15，16+)
    """

    def stat(self):

        sql = """ delete from dcnew.loss_user_paycnt_dis_general where fdate = '%(stat_date)s';

        insert into dcnew.loss_user_paycnt_dis_general
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fchannelcode
               ,case when nvl(fpay_cnt,0) <=2 then cast (fpay_cnt as varchar)
                     when fpay_cnt >=3 and fpay_cnt <=5 then '3-5'
                     when fpay_cnt >=6 and fpay_cnt <=10 then '6-10'
                     when fpay_cnt >=11 and fpay_cnt <=15 then '11-15'
                     when fpay_cnt >15 then '16+' end fpay_cnt
               ,flossdays
               ,sum(fcloss_unum) fcloss_unum
               ,sum(fdloss_unum ) fdloss_unum
          FROM dcnew.loss_user_paycnt_dis
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
                 ,case when nvl(fpay_cnt,0) <=2 then cast (fpay_cnt as varchar)
                       when fpay_cnt >=3 and fpay_cnt <=5 then '3-5'
                       when fpay_cnt >=6 and fpay_cnt <=10 then '6-10'
                       when fpay_cnt >=11 and fpay_cnt <=15 then '11-15'
                       when fpay_cnt >15 then '16+' end;

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_loss_user_paycnt_dis_sp(stat_date)
    a()
