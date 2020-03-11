#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_gameparty_settlement_sp(BasePGCluster):

    """拆分结果表 dcnew.gameparty_settlement
       分为 dcnew.gameparty_settlement_ante(fsubname = '-21379')
         与 dcnew.gameparty_settlement_subname(fante = '-21379')
    """

    def stat(self):

        sql = """ delete from dcnew.gameparty_settlement_ante where fdate = '%(stat_date)s';

        insert into dcnew.gameparty_settlement_ante
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fchannelcode
               ,fante
               ,fpname
               ,fsubname
               ,fplay_unum
               ,fbank_unum
               ,fparty_num
               ,fwinplayer_cnt
               ,floseplayer_cnt
               ,fbankparty_num
               ,fwingc_sum
               ,flosegc_sum
               ,fwingc_avg
               ,flosegc_avg
               ,fcharge
               ,fmultiple_avg
               ,f1bankrupt_num
               ,f2bankrupt_num
               ,fbankuser_cnt
               ,frb_num
               ,frb_win_coins
               ,frb_lost_coins
               ,frb_win_party
               ,frb_party
               ,fbankpay_unum
               ,fbankusercnt
               ,f10bankpay_unum
          FROM dcnew.gameparty_settlement
         where fdate = '%(stat_date)s'
           and fsubname = '-21379';

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)

        sql = """ delete from dcnew.gameparty_settlement_subname where fdate = '%(stat_date)s';

        insert into dcnew.gameparty_settlement_subname
        SELECT  fdate
               ,fgamefsk
               ,fplatformfsk
               ,fhallfsk
               ,fsubgamefsk
               ,fterminaltypefsk
               ,fversionfsk
               ,fchannelcode
               ,fante
               ,fpname
               ,fsubname
               ,fplay_unum
               ,fbank_unum
               ,fparty_num
               ,fwinplayer_cnt
               ,floseplayer_cnt
               ,fbankparty_num
               ,fwingc_sum
               ,flosegc_sum
               ,fwingc_avg
               ,flosegc_avg
               ,fcharge
               ,fmultiple_avg
               ,f1bankrupt_num
               ,f2bankrupt_num
               ,fbankuser_cnt
               ,frb_num
               ,frb_win_coins
               ,frb_lost_coins
               ,frb_win_party
               ,frb_party
               ,fbankpay_unum
               ,fbankusercnt
               ,f10bankpay_unum
          FROM dcnew.gameparty_settlement
         where fdate = '%(stat_date)s'
           and fante = '-21379';

        COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_gameparty_settlement_sp(stat_date)
    a()
