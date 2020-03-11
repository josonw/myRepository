#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date


class agg_user_province_online_user_m_info(BasePGCluster):

    """地方棋牌分省数据增加在线在玩
    """

    def stat(self):

        sql = """
              delete from dcnew.province_user_monthly_info where fdate= '%(ld_month_begin)s' and fmax_online <>0;

                 COMMIT;
        """ % self.sql_dict
        self.exe_hql(sql)



        sql = """

      insert into dcnew.province_user_monthly_info
      select '%(ld_month_begin)s' fdate,
             fgamefsk,
             fplatformfsk,
             fhallfsk,
             fsubgamefsk,
             fterminaltypefsk,
             fversionfsk,
             fchannelcode,
             fprovince,
             0 fact_unum,
             0 freg_unum,
             fmax_online,
             fmax_play,
             0 fldr_back_unum,
             0 fgame_back_unum,
             0 fhall_back_unum,
             0 fpart_reg_unum,
             0 fpart_back_unum,
             0 fpart_ind_reg_unum,
             0 fpart_ind_back_unum,
             0 fshare_unum,
             0 fshare_reg_unum,
             0 fgame_act_unum,
             favg_online,
             favg_play
        from (select fgamefsk,
                     fplatformfsk,
                     fhallfsk,
                     fsubgamefsk,
                     fterminaltypefsk,
                     fversionfsk,
                     fchannelcode,
                     fprovince,
                     max(fmax_online) fmax_online,
                     max(fmax_play) fmax_play,
                     avg(favg_online) favg_online,
                     avg(favg_play) favg_play
                from dcnew.province_user_info
               where fdate between '%(ld_month_begin)s' and '%(ld_begin)s'
               group by fgamefsk,
                        fplatformfsk,
                        fhallfsk,
                        fsubgamefsk,
                        fterminaltypefsk,
                        fversionfsk,
                        fchannelcode,
                        fprovince
               ) t """ % self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_user_province_online_user_m_info(stat_date)
    a()
