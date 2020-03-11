#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date

class promotion_channel_online(BasePGCluster):
    """用户在线在玩数据
    """

    def stat(self):

        sql = """delete from analysis.user_channel_promo_online_mid
                  where fdate >= date'%(ld_begin)s' and fdate < date'%(ld_end)s' """ % self.sql_dict
        self.append(sql)

        sql = """
            insert into analysis.user_channel_promo_online_mid(fdate, fbpid, fchannel_id, fonline_num, fplay_num)
            select date_trunc('minute', fdate) fdate,
                   fbpid,
                   b.ftrader_id fchannel_id,
                   sum(fonline_num) fonline_num,
                   sum(fplay_num) fplay_num
              from analysis.user_online_channel_fct a
              join analysis.marketing_channel_pkg_info b
                on a.fchannel_pkg_id = cast(b.fid AS varchar(64))
             where fdate >= date'%(ld_begin)s'
               and fdate < date'%(ld_end)s'
             group by date_trunc('minute', fdate), fbpid, b.ftrader_id
        """ % self.sql_dict
        self.append(sql)


        sql = """delete from analysis.user_channel_promo_online_fct
                  where fdate >= date'%(ld_begin)s' and fdate < date'%(ld_end)s' """ % self.sql_dict
        self.append(sql)

        sql = """
             insert into analysis.user_channel_promo_online_fct(fdate, fgamefsk, fplatformfsk, fversionfsk, fchannel_id,
                                              fmax_online, favg_online, fsum_online, fmax_play, favg_play, fsum_play, fmin_online, fmin_play)
            select date'%(ld_begin)s' fdate,
                   fgamefsk,
                   fplatformfsk,
                   fversionfsk,
                   fchannel_id,
                   max(fonline_num) fmax_online,
                   round(avg(fonline_num), 0) favg_online,
                   sum(fonline_num) fsum_online,
                   max(fplay_num) fmax_play,
                   round(avg(fplay_num), 0) favg_play,
                   sum(fplay_num) fsum_play,
                   min(fonline_num) fmin_online,
                   min(fplay_num) fmin_play
              from analysis.user_channel_promo_online_mid a
              join analysis.bpid_platform_game_ver_map b
                on a.fbpid = b.fbpid
             where a.fdate >= date'%(ld_begin)s'
               and a.fdate <  date'%(ld_end)s'
             group by fgamefsk, fplatformfsk, fversionfsk, fchannel_id
        """ % self.sql_dict
        self.append(sql)

        self.exe_hql_list()


if __name__ == "__main__":

    stat_date = get_stat_date()
    #生成统计实例
    a = promotion_channel_online(stat_date)
    a()
