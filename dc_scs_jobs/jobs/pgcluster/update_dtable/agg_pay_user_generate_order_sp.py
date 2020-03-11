#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_pay_user_generate_order_sp(BasePGCluster):

    """拆分结果表 dcnew.pay_user_generate_order
       分为 dcnew.pay_user_generate_order_detail(pname <> '-21379')
         与 dcnew.pay_user_generate_order_general(pname = '-21379')
    """

    def stat(self):

        sql = """ delete from dcnew.pay_user_generate_order_general where fdate = '%(stat_date)s';

        insert into dcnew.pay_user_generate_order_general
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fchannelcode
               ,fpname
               ,fsubname
               ,fante
               ,fpay_scene
               ,fis_bankrupt
               ,fuser_type
               ,fpm_name
               ,forder_cnt
               ,forder_unum
               ,fpay_cnt
               ,fpay_unum
               ,fincome
          FROM dcnew.pay_user_generate_order
         where fdate = '%(stat_date)s'
           and fpname = '-21379';

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

        sql = """ delete from dcnew.pay_user_generate_order_detail where fdate = '%(stat_date)s';

        insert into dcnew.pay_user_generate_order_detail
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fchannelcode
               ,fpname
               ,fsubname
               ,fante
               ,fpay_scene
               ,fis_bankrupt
               ,fuser_type
               ,fpm_name
               ,forder_cnt
               ,forder_unum
               ,fpay_cnt
               ,fpay_unum
               ,fincome
          FROM dcnew.pay_user_generate_order
         where fdate = '%(stat_date)s'
           and fpname <> '-21379';

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_pay_user_generate_order_sp(stat_date)
    a()
