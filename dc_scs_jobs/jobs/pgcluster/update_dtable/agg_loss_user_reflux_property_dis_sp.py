#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_loss_user_reflux_property_dis_sp(BasePGCluster):

    """拆分结果表 dcnew.loss_user_reflux_property_dis
       分为 dcnew.loss_user_reflux_property_dis_general(行转列)
    """

    def stat(self):

        sql = """ delete from dcnew.loss_user_reflux_property_dis_general where fdate = '%(stat_date)s';

        insert into dcnew.loss_user_reflux_property_dis_general
        select  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fchannelcode
               ,flossdays
               ,sum(case when fproperty = '0' then freflux_unum else 0 end) feq0
               ,sum(case when fproperty = '1' then freflux_unum else 0 end) fm1lq1k
               ,sum(case when fproperty = '1000' then freflux_unum else 0 end) fm1klq5k
               ,sum(case when fproperty = '5000' then freflux_unum else 0 end) fm5klq10k
               ,sum(case when fproperty = '10000' then freflux_unum else 0 end) fm10klq50k
               ,sum(case when fproperty = '50000' then freflux_unum else 0 end) fm50klq100k
               ,sum(case when fproperty = '100000' then freflux_unum else 0 end) fm100klq500k
               ,sum(case when fproperty = '500000' then freflux_unum else 0 end) fm500klq1m
               ,sum(case when fproperty = '1000000' then freflux_unum else 0 end) fm1mlq5m
               ,sum(case when fproperty = '5000000' then freflux_unum else 0 end) fm5mlq10m
               ,sum(case when fproperty = '10000000' then freflux_unum else 0 end) fm10mlq50m
               ,sum(case when fproperty = '50000000' then freflux_unum else 0 end) fm50mlq100m
               ,sum(case when fproperty = '100000000' then freflux_unum else 0 end) fm100mlq1b
               ,sum(case when fproperty = '1000000000' then freflux_unum else 0 end) fm1b
          FROM dcnew.loss_user_reflux_property_dis a
         where fdate = '%(stat_date)s'
         group by fdate
                 ,fgamefsk
                 ,fplatformfsk
                 ,fhallfsk
                 ,fsubgamefsk
                 ,fterminaltypefsk
                 ,fversionfsk
                 ,fchannelcode
                 ,flossdays;

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_loss_user_reflux_property_dis_sp(stat_date)
    a()
