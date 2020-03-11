#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_loss_user_gamecoin_dis_sp(BasePGCluster):

    """拆分结果表 dcnew.loss_user_gamecoin_dis
       分为 dcnew.loss_user_gamecoin_dis_general(行转列)
    """

    def stat(self):

        sql = """ delete from dcnew.loss_user_gamecoin_dis_general where fdate = '%(stat_date)s';

        insert into dcnew.loss_user_gamecoin_dis_general
        select  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fchannelcode
               ,flossdays
               ,coalesce(sum(case when fcoin_num = 0 then fdloss_unum end), 0) num_0
               ,coalesce(sum(case when fcoin_num = 1 then fdloss_unum end), 0) num_1
               ,coalesce(sum(case when fcoin_num = 1000 then fdloss_unum end), 0) num_1000
               ,coalesce(sum(case when fcoin_num = 5000 then fdloss_unum end), 0) num_5000
               ,coalesce(sum(case when fcoin_num = 10000 then fdloss_unum end), 0) num_10000
               ,coalesce(sum(case when fcoin_num = 50000 then fdloss_unum end), 0) num_50000
               ,coalesce(sum(case when fcoin_num = 100000 then fdloss_unum end), 0) num_100000
               ,coalesce(sum(case when fcoin_num = 500000 then fdloss_unum end), 0) num_500000
               ,coalesce(sum(case when fcoin_num = 1000000 then fdloss_unum end), 0) num_1000000
               ,coalesce(sum(case when fcoin_num = 5000000 then fdloss_unum end), 0) num_5000000
               ,coalesce(sum(case when fcoin_num = 10000000 then fdloss_unum end), 0) num_10000000
               ,coalesce(sum(case when fcoin_num = 50000000 then fdloss_unum end), 0) num_50000000
               ,coalesce(sum(case when fcoin_num = 100000000 then fdloss_unum end), 0) num_100000000
               ,coalesce(sum(case when fcoin_num = 1000000000 then fdloss_unum end), 0) num_1000000000
          FROM dcnew.loss_user_gamecoin_dis a
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
    a = agg_loss_user_gamecoin_dis_sp(stat_date)
    a()
