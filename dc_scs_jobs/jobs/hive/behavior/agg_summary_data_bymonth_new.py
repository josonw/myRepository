#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date

class agg_summary_game_data_by_month(BasePGStat):

    def stat(self):
        sql = """
            create table if not exists dcnew.summary_games_month_data (
                fdate DATE,
                fgamefsk BIGINT,
                fplatformfsk BIGINT,
                fhallfsk BIGINT,
                fsubgamefsk BIGINT,
                fterminaltypefsk BIGINT,
                fversionfsk BIGINT,
                fchannelcode BIGINT,

                mau            bigint,             --月活跃人数
                msu            bigint,             --月新增人数
                mau_da           decimal(38,4),          --月日均活跃人数
                msu_da           decimal(38,4),          --月日均新增人数

                mpun            bigint,            --月玩牌人数
                mpun_da          decimal(38,4),         --月日均玩牌人数

                mpu            bigint,             --月付费人数
                mip            decimal(38,4),      --月付费额
                mpu_da            decimal(38,4),          --月日均付费人数
                mip_da            decimal(38,4),   --月日均付费额

                maou_da          decimal(38,4),         --日均在线均值
                maopu_da          decimal(38,4)          --日均在玩均值
            );
        """ % self.sql_dict
        self.exe_hql(sql)

        sql_dict = {
            'ld_begin':self.stat_date,
            'month_begin':self.stat_date.split('-')[0] + '-' + self.stat_date.split('-')[1],
        }

        # 删除当天数据，避免重复
        sql = """
            delete from dcnew.summary_games_month_data
            where  fdate = TO_DATE('%(month_begin)s','yyyy-mm');
          commit;
        """ % sql_dict
        self.exe_hql(sql)

        sql = """
            insert into dcnew.summary_games_month_data
            SELECT
                TO_DATE('%(month_begin)s','yyyy-mm') fdate,
                fgamefsk,
                fplatformfsk,
                fhallfsk,
                fsubgamefsk,
                fterminaltypefsk,
                fversionfsk,
                fchannelcode,
                max(mau) mau ,
                max(mau_da) mau_da ,
                max(msu) msu ,
                max(msu_da) msu_da ,
                max(mpun) mpun ,
                max(mpun_da) mpun_da ,
                max(mpu) mpu ,
                max(mip) mip ,
                max(mpu_da) mpu_da ,
                max(mip_da) mip_da ,
                max(maou_da) maou_da ,
                max(maopu_da) maopu_da

            from (
                -- 月活跃用户数、日均活跃用户数
                SELECT
                    fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    max(fmaucnt) mau, avg(coalesce(fdactucnt, 0)) mau_da,
                    0 msu, 0 msu_da, 0 mpun, 0 mpun_da, 0 mpu, 0 mip,
                    0 mpu_da, 0 mip_da, 0 maou_da, 0 maopu_da
                FROM dcnew.act_user
                WHERE fdate >= date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD'))
                    AND fdate < date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD') + '1 month'::interval)
                GROUP BY fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk,
                    fterminaltypefsk, fversionfsk, fchannelcode

                UNION ALL

                -- 月新增用户数、日均新增用户数
                SELECT
                    fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    0, 0, sum(fdregucnt) msu, avg(fdregucnt) msu_da,
                    0, 0, 0, 0, 0, 0, 0, 0
                FROM
                    dcnew.reg_user
                WHERE fdate >= date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD'))
                    AND fdate < date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD') + '1 month'::interval)
                GROUP BY fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk,
                    fterminaltypefsk, fversionfsk, fchannelcode

                UNION ALL

                -- 月玩牌用户数、日均玩牌用户数
                SELECT
                    fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    0, 0, 0, 0,
                    sum(fplayusernum) mpun, avg(fplayusernum) mpun_da,
                    0, 0, 0, 0, 0, 0
                FROM
                    dcnew.gameparty_total
                WHERE fdate >= date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD'))
                    AND fdate < date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD') + '1 month'::interval)
                GROUP BY fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk,
                    fterminaltypefsk, fversionfsk, fchannelcode

                UNION ALL

                -- 月付费用户数、日均付费用户数
                -- 月付费金额、日均付费金额
                SELECT
                    fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    0, 0, 0, 0,
                    0, 0,
                    max(fmonthpayusercnt) mpu, sum(fincome) mip,
                    avg(fpayusercnt) mpu_da, avg(fincome) mip_da,
                    0, 0
                FROM
                    dcnew.pay_user_income
                WHERE fdate >= date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD'))
                    AND fdate < date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD') + '1 month'::interval)
                GROUP BY fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk,
                    fterminaltypefsk, fversionfsk, fchannelcode

                UNION ALL

                -- 日均在玩均值、日均在线均值
                SELECT
                    fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk, fterminaltypefsk, fversionfsk, fchannelcode,
                    0, 0, 0, 0,
                    0, 0, 0, 0,
                    0, 0,
                    avg(favgonline) maou_da, avg(favgplay) maopu_da
                FROM
                    dcnew.user_online_byday_new
                WHERE fdate >= date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD'))
                    AND fdate < date_trunc('MONTH', TO_DATE('%(ld_begin)s', 'YYYY-MM-DD') + '1 month'::interval)
                GROUP BY fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk,
                    fterminaltypefsk, fversionfsk, fchannelcode

            ) AS foo
            GROUP BY fgamefsk, fplatformfsk, fhallfsk, fsubgamefsk,
                    fterminaltypefsk, fversionfsk, fchannelcode;

            commit;
        """% sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = agg_summary_game_data_by_month(stat_date)
    a()

