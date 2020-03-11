#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_loss_user_rupt_dis_sp(BasePGCluster):

    """拆分结果表 dcnew.loss_user_rupt_dis
       分为 dcnew.loss_user_rupt_dis_general(付费额度分段：0次，1次，2次，3次，4次，5次，5+次)
    """

    def stat(self):

        sql = """ delete from dcnew.loss_user_rupt_dis_general where fdate = '%(stat_date)s';

        insert into dcnew.loss_user_rupt_dis_general
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fchannelcode
               ,case when coalesce(frupt_cnt,0) = 0 then '0次'
                     when frupt_cnt = 1 then '1次'
                     when frupt_cnt = 2 then '2次'
                     when frupt_cnt = 3 then '3次'
                     when frupt_cnt = 4 then '4次'
                     when frupt_cnt = 5 then '5次'
                     when frupt_cnt > 5 then '5+次' end frupt_cnt
               ,flossdays
               ,sum(fcloss_unum) fcloss_unum
               ,sum(fdloss_unum) fdloss_unum
          FROM dcnew.loss_user_rupt_dis
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
                 ,case when coalesce(frupt_cnt,0) = 0 then '0次'
                       when frupt_cnt = 1 then '1次'
                       when frupt_cnt = 2 then '2次'
                       when frupt_cnt = 3 then '3次'
                       when frupt_cnt = 4 then '4次'
                       when frupt_cnt = 5 then '5次'
                       when frupt_cnt > 5 then '5+次' end;

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_loss_user_rupt_dis_sp(stat_date)
    a()
