#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date

class agg_summary_data_by_month(BasePGStat):

    def stat(self):
 #        sql = """
 #       create table if not exists analysis.summary_data_by_month_fct
 #       (
 #           fdate          date  ,
 #           fgamefsk       bigint  ,
 #           fplatformfsk   bigint  ,
 #           fversionfsk    bigint  ,
 #           mau            bigint,             --月活跃人数
 #           msu            bigint,             --月新增人数
 #           mau_da           decimal(38,4),          --月日均活跃人数
 #           msu_da           decimal(38,4),          --月日均新增人数

 #           mpun            bigint,            --月玩牌人数
 #           mpun_da          decimal(38,4),         --月日均玩牌人数

 #           mpu            bigint,             --月付费人数
 #           mip            decimal(38,4),      --月付费额
 #           mpu_da            decimal(38,4),          --月日均付费人数
 #           mip_da            decimal(38,4),   --月日均付费额

 #           maou_da          decimal(38,4)          --日均在线均值
 # )
 #        """ % self.sql_dict
 #        self.exe_hql(sql)

        sql_dict = {
        'ld_begin':self.stat_date,
        'month_begin':self.stat_date.split('-')[0] + '-' + self.stat_date.split('-')[1],
        }

        # 删除当天数据，避免重复
        sql = """
        delete from analysis.summary_data_by_month_fct
        where  fdate = TO_DATE('%(month_begin)s','yyyy-mm')
        ;

        commit;
        """ % sql_dict
        self.exe_hql(sql)



        sql = """
        insert into analysis.summary_data_by_month_fct

        SELECT TO_DATE('%(month_begin)s','yyyy-mm') fdate,
               fgamefsk,
               fplatformfsk,
               fversionfsk,
               max(mau) mau ,
               max(msu) msu ,
               max(mau_da) mau_da ,
               max(msu_da) msu_da ,
               max(mpun) mpun ,
               max(mpun_da) mpun_da ,
               max(mpu) mpu ,
               max(mip) mip ,
               max(mpu_da) mpu_da ,
               max(mip_da) mip_da ,
               max(maou_da) maou_da ,
               max(maopu_da) maopu_da
               from
          ( SELECT fgamefsk, fplatformfsk, fversionfsk, MAX(coalesce(fmonthactcnt,0)) mau, 0 msu, avg(coalesce(factcnt, 0)) mau_da, 0 msu_da, 0 mpun, 0 mpun_da, 0 mpu, 0 mip, 0 mpu_da, 0 mip_da, 0 maou_da, 0 maopu_da
           FROM analysis.user_true_active_fct
           WHERE fdate >= date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD'))
             AND fdate < date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD') + '1 month'::interval)
           GROUP BY fgamefsk, fplatformfsk, fversionfsk

           UNION ALL

            SELECT fgamefsk,
                   fplatformfsk,
                   fversionfsk,
                   0, sum(coalesce(fregcnt, 0)) msu, 0, avg(coalesce(fregcnt, 0)), 0, 0, 0, 0, 0, 0, 0, 0 maopu_da
            FROM
                  (SELECT fdate, fgamefsk, fplatformfsk, fversionfsk, sum(coalesce(fregcnt, 0)) fregcnt
                   FROM analysis.user_register_fct
                   WHERE fdate >= date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD'))
                     AND fdate < date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD') + '1 month'::interval)
                   GROUP BY fdate, fgamefsk, fplatformfsk, fversionfsk) AS foo
            GROUP BY fgamefsk,
                     fplatformfsk,
                     fversionfsk

           UNION ALL

           SELECT fgamefsk, fplatformfsk, fversionfsk, 0,0,0,0, sum(coalesce(fplayusernum, 0)) mpun, avg(coalesce(fplayusernum, 0)) mpun_da, 0,0,0,0,0, 0 maopu_da
           FROM analysis.user_gameparty_game_fct
           WHERE fdate >= date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD'))
             AND fdate < date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD') + '1 month'::interval)
           GROUP BY fgamefsk, fplatformfsk, fversionfsk

           UNION ALL

           SELECT fgamefsk, fplatformfsk, fversionfsk, 0,0,0,0,0,0, max(coalesce(fmonthpayusercnt,0)) mpu, sum(coalesce(fincome,0)) mip, avg(coalesce(fpayusercnt,0)) mpu_da, avg(coalesce(fincome,0)) mip_da, 0, 0 maopu_da
           FROM analysis.user_payment_fct
           WHERE fdate >= date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD'))
             AND fdate < date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD') + '1 month'::interval)
           GROUP BY fgamefsk, fplatformfsk, fversionfsk

           UNION  ALL

           SELECT fgamefsk, fplatformfsk, fversionfsk, 0,0,0,0,0,0,0,0,0,0, avg(coalesce(favgonline,0)) maou_da, avg(coalesce(favgplay,0)) maopu_da
           FROM analysis.user_online_byday_agg
           WHERE fdate >= date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD'))
             AND fdate < date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD') + '1 month'::interval)
             and (coalesce(favgonline,0) !=0 or coalesce(favgplay,0) !=0)
           GROUP BY fgamefsk, fplatformfsk, fversionfsk) AS foo
        GROUP BY fgamefsk,
                 fplatformfsk,
                 fversionfsk;
        commit;

        """% sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":
    stat_date = get_stat_date()
    a = agg_summary_data_by_month(stat_date)
    a()
