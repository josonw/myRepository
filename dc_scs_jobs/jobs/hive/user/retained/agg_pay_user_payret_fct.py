#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BasePGStat
import service.sql_const as sql_const


class agg_pay_user_payret_fct(BaseStatModel):
    """付费用户，在其后一段时间内的付费留存
    """
    def create_tab(self):
        pass

    def stat(self):
        """ 留存用户的新计算方法， 在pg打横"""

        self.sql_dict.update(PublicFunc.date_define(self.stat_date))

        sql = """INSERT INTO dcnew.pay_user_payret_fct
        (fdate,
         fgamefsk,
         fplatformfsk,
         fhallfsk,
         fsubgamefsk,
         fterminaltypefsk,
         fversionfsk,
         fchannelcode,
         f1daycnt,
         f2daycnt,
         f3daycnt,
         f4daycnt,
         f5daycnt,
         f6daycnt,
         f7daycnt,
         f14daycnt,
         f30daycnt)

        select fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
        max(f1daycnt) f1daycnt,
        max(f2daycnt) f2daycnt,
        max(f3daycnt) f3daycnt,
        max(f4daycnt) f4daycnt,
        max(f5daycnt) f5daycnt,
        max(f6daycnt) f6daycnt,
        max(f7daycnt) f7daycnt,
        max(f14daycnt) f14daycnt,
        max(f30daycnt) f30daycnt
        from (
        select fpaydate fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                if( retday=1, retusernum, 0 ) f1daycnt,
                if( retday=2, retusernum, 0 ) f2daycnt,
                if( retday=3, retusernum, 0 ) f3daycnt,
                if( retday=4, retusernum, 0 ) f4daycnt,
                if( retday=5, retusernum, 0 ) f5daycnt,
                if( retday=6, retusernum, 0 ) f6daycnt,
                if( retday=7, retusernum, 0 ) f7daycnt,
                if( retday=14, retusernum, 0 ) f14daycnt,
                if( retday=30, retusernum, 0 ) f30daycnt
            from dcnew.pay_user_payret
            where fpaydate < '%(ld_dayend)s' and fpaydate >= '%(ld_30dayago)s')
            union all
            SELECT fdate,fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                   f1daycnt,
                   f2daycnt,
                   f3daycnt,
                   f4daycnt,
                   f5daycnt,
                   f6daycnt,
                   f7daycnt,
                   f14daycnt,
                   f30daycnt
            FROM  dcnew.pay_user_actret
            WHERE dt >= '%(ld_30dayago)s'
              AND dt < '%(ld_dayend)s'
        ) tmp group by fdate, fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode

        """% self.sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":
    stat_date = get_stat_date()
    a = agg_pay_user_payret_fct(stat_date)
    a()
