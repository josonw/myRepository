#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date

class agg_summary_data_by_day(BasePGStat):

    def stat(self):
        # sql = """
        # create table if not exists analysis.summary_data_by_day_fct
        # (
        #     fdate          date  ,
        #     fgamefsk       bigint  ,
        #     fplatformfsk   bigint  ,
        #     fversionfsk    bigint  ,
        #     dau            bigint,
        #     dsu            bigint,
        #     pun            bigint,          --玩牌人数
        #     dpu            bigint,          --付费人数
        #     dip            decimal(38,2),   --付费额
        #     daou           bigint,          --在线均值
        # )
        # """ % self.sql_dict
        # self.exe_hql(sql)



        # 删除当天数据，避免重复
        sql = """
        delete from analysis.summary_data_by_day_fct
        where  fdate >= date'%(ld_begin)s'
        AND fdate < date'%(ld_end)s';

        commit;
        """ % self.sql_dict
        self.exe_hql(sql)

        sql = """
        --活跃
         insert into analysis.summary_data_by_day_fct
         (fdate,fgamefsk,fplatformfsk,fversionfsk,dau)
         SELECT '%(ld_begin)s' fdate,fgamefsk,
                                  fplatformfsk,
                                  fversionfsk,
                                  sum(coalesce(factcnt, 0)) dau
                           FROM analysis.user_true_active_fct
                           WHERE fdate >= date'%(ld_begin)s'
                             AND fdate < date'%(ld_end)s'
                           GROUP BY fgamefsk,
                                    fplatformfsk,
                                    fversionfsk;
        commit;
        """% self.sql_dict
        self.exe_hql(sql)

        sql = """
                 --新增
                 WITH datas as
                  (SELECT date'%(ld_begin)s' fdate, fgamefsk, fplatformfsk, fversionfsk, sum(coalesce(fregcnt, 0)) dsu
                   FROM analysis.user_register_fct
                   WHERE fdate >= date'%(ld_begin)s'
                     AND fdate < date'%(ld_end)s'
                   GROUP BY fgamefsk, fplatformfsk, fversionfsk),
                      updated AS
                  ( UPDATE analysis.summary_data_by_day_fct AS t
                   SET dsu = a.dsu
                   FROM datas AS a
                   WHERE t.fdate=a.fdate
                     AND a.fgamefsk =t.fgamefsk
                     AND a.fplatformfsk=t.fplatformfsk
                     AND a.fversionfsk=t.fversionfsk
                     returning t.fdate, t.fgamefsk, t.fplatformfsk, t.fversionfsk,t.dsu
                     )
                INSERT INTO analysis.summary_data_by_day_fct( fdate,fgamefsk,fplatformfsk,fversionfsk,dsu)
                SELECT a.*
                FROM datas a
                LEFT JOIN updated t ON t.fdate=a.fdate
                AND a.fgamefsk =t.fgamefsk
                AND a.fplatformfsk=t.fplatformfsk
                AND a.fversionfsk=t.fversionfsk
                WHERE t.fgamefsk IS NULL;
                commit;
        """% self.sql_dict
        self.exe_hql(sql)



        sql = """
                 --玩牌
                 WITH datas as
                  (SELECT date'%(ld_begin)s' fdate, fgamefsk, fplatformfsk, fversionfsk,
                    sum(coalesce(fplayusernum, 0)) pun
                   FROM analysis.user_gameparty_game_fct
                   WHERE fdate >= date'%(ld_begin)s'
                     AND fdate < date'%(ld_end)s'
                   GROUP BY fgamefsk, fplatformfsk, fversionfsk),
                      updated AS
                  ( UPDATE analysis.summary_data_by_day_fct AS t
                   SET pun = a.pun
                   FROM datas AS a
                   WHERE t.fdate=a.fdate
                     AND a.fgamefsk =t.fgamefsk
                     AND a.fplatformfsk=t.fplatformfsk
                     AND a.fversionfsk=t.fversionfsk
                     returning t.fdate, t.fgamefsk, t.fplatformfsk, t.fversionfsk,t.pun )
                INSERT INTO analysis.summary_data_by_day_fct( fdate,fgamefsk,fplatformfsk,fversionfsk,pun)
                SELECT a.*
                FROM datas a
                LEFT JOIN updated t ON t.fdate=a.fdate
                AND a.fgamefsk =t.fgamefsk
                AND a.fplatformfsk=t.fplatformfsk
                AND a.fversionfsk=t.fversionfsk
                WHERE t.fgamefsk IS NULL;
                commit;
        """% self.sql_dict
        self.exe_hql(sql)


        sql = """
              --付费
             WITH datas as
              (SELECT date'%(ld_begin)s' fdate, fgamefsk, fplatformfsk, fversionfsk,
                sum(fpayusercnt) dpu, sum(fincome) dip
               FROM analysis.user_payment_fct
               WHERE fdate >= date'%(ld_begin)s'
                 AND fdate < date'%(ld_end)s'
               GROUP BY fgamefsk, fplatformfsk, fversionfsk),
                  updated AS
              (UPDATE analysis.summary_data_by_day_fct AS t
               SET dpu = a.dpu,
                   dip = a.dip
               FROM datas AS a
               WHERE t.fdate=a.fdate
                 AND a.fgamefsk =t.fgamefsk
                 AND a.fplatformfsk=t.fplatformfsk
                 AND a.fversionfsk=t.fversionfsk
                 returning t.fdate, t.fgamefsk, t.fplatformfsk, t.fversionfsk,t.dpu,t.dip )
            INSERT INTO analysis.summary_data_by_day_fct(fdate,fgamefsk,fplatformfsk,fversionfsk,dpu,dip)
            SELECT a.*
            FROM datas a
            LEFT JOIN updated t ON t.fdate=a.fdate
            AND a.fgamefsk =t.fgamefsk
            AND a.fplatformfsk=t.fplatformfsk
            AND a.fversionfsk=t.fversionfsk
            WHERE t.fgamefsk IS NULL;
                commit;
        """% self.sql_dict
        self.exe_hql(sql)


        sql = """
                 --在线在玩
                 WITH datas AS
                  (SELECT date'%(ld_begin)s' fdate,
                                             fgamefsk,
                                             fplatformfsk,
                                             fversionfsk,
                                             avg(favgonline) daou,
                                             avg(favgplay) daopu
                   FROM analysis.user_online_byday_agg
                   WHERE fdate >= date'%(ld_begin)s'
                     AND fdate < date'%(ld_end)s'
                     and (coalesce(favgonline,0) !=0 or coalesce(favgplay,0) !=0)
                   GROUP BY fgamefsk,
                            fplatformfsk,
                            fversionfsk),
                     updated AS
                  (UPDATE analysis.summary_data_by_day_fct AS t
                   SET daou = a.daou,daopu = a.daopu
                   FROM datas AS a
                   WHERE t.fdate=a.fdate
                     AND a.fgamefsk =t.fgamefsk
                     AND a.fplatformfsk=t.fplatformfsk
                     AND a.fversionfsk=t.fversionfsk returning t.fdate,
                                                               t.fgamefsk,
                                                               t.fplatformfsk,
                                                               t.fversionfsk,
                                                               t.daou,t.daopu)
                INSERT INTO analysis.summary_data_by_day_fct(fdate,fgamefsk,fplatformfsk,fversionfsk,daou,daopu)
                SELECT a.*
                FROM datas a
                LEFT JOIN updated t
                ON t.fdate=a.fdate
                AND a.fgamefsk =t.fgamefsk
                AND a.fplatformfsk=t.fplatformfsk
                AND a.fversionfsk=t.fversionfsk
                WHERE t.fgamefsk IS NULL;

                 COMMIT;
        """% self.sql_dict
        self.exe_hql(sql)

        # sql = """
        #         insert into analysis.summary_data_by_day_fct
        #                 SELECT '%(ld_begin)s' fdate,
        #                                       a.fgamefsk,
        #                                       a.fplatformfsk,
        #                                       a.fversionfsk,
        #                                       a.fterminalfsk,
        #                                       coalesce(dau, 0) dau,
        #                                       coalesce(dsu, 0) dsu,
        #                                       coalesce(pun, 0) pun,
        #                                       coalesce(dpu, 0) dpu,
        #                                       coalesce(dip, 0) dip,
        #                                       coalesce(daou, 0) daou,
        #                                       coalesce(dmou, 0) dmou,
        #                                       coalesce(daopu, 0) daopu,
        #                                       coalesce(dmopu, 0) dmopu
        #                 FROM
        #                   (SELECT fgamefsk,
        #                           fplatformfsk,
        #                           fversionfsk,
        #                           fterminalfsk,
        #                           sum(coalesce(fregcnt, 0)) dsu,
        #                           sum(coalesce(factcnt, 0)) dau
        #                    FROM analysis.user_true_active_fct
        #                    WHERE fdate >= date'%(ld_begin)s'
        #                      AND fdate < date'%(ld_end)s'
        #                    GROUP BY fgamefsk,
        #                             fplatformfsk,
        #                             fversionfsk,
        #                             fterminalfsk) a
        #                 LEFT JOIN
        #                   (SELECT fgamefsk,
        #                           fplatformfsk,
        #                           fversionfsk,
        #                           fterminalfsk,
        #                           sum(coalesce(fplayusernum, 0)) pun
        #                    FROM analysis.user_gameparty_game_fct
        #                    WHERE fdate >= date'%(ld_begin)s'
        #                      AND fdate < date'%(ld_end)s'
        #                    GROUP BY fgamefsk,
        #                             fplatformfsk,
        #                             fversionfsk,
        #                             fterminalfsk) b ON a.fgamefsk=b.fgamefsk
        #                 AND a.fplatformfsk=b.fplatformfsk
        #                 AND a.fversionfsk=b.fversionfsk
        #                 LEFT JOIN
        #                   (SELECT fgamefsk,
        #                           fplatformfsk,
        #                           fversionfsk,
        #                           fterminalfsk ,
        #                           sum(fpayusercnt) dpu,
        #                           sum(fincome) dip
        #                    FROM analysis.user_payment_fct
        #                    WHERE fdate >= date'%(ld_begin)s'
        #                      AND fdate < date'%(ld_end)s'
        #                    GROUP BY fgamefsk,
        #                             fplatformfsk,
        #                             fversionfsk,
        #                             fterminalfsk) c ON a.fgamefsk=c.fgamefsk
        #                 AND a.fplatformfsk=c.fplatformfsk
        #                 AND a.fversionfsk=c.fversionfsk
        #                 LEFT JOIN
        #                   (SELECT fgamefsk,
        #                           fplatformfsk,
        #                           fversionfsk,
        #                           fterminalfsk ,
        #                           max(favgonline) daou,
        #                           max(fmaxonline) dmou,
        #                           max(favgplay) daopu,
        #                           max(fmaxplay) dmopu
        #                    FROM analysis.user_online_byday_agg
        #                    WHERE fdate >= date'%(ld_begin)s'
        #                      AND fdate < date'%(ld_end)s'
        #                    GROUP BY fgamefsk,
        #                             fplatformfsk,
        #                             fversionfsk,
        #                             fterminalfsk) d ON a.fgamefsk=d.fgamefsk
        #                 AND a.fplatformfsk=d.fplatformfsk
        #                 AND a.fversionfsk=d.fversionfsk;
        # commit;
        # """% self.sql_dict
        # self.exe_hql(sql)

if __name__ == "__main__":
    stat_date = get_stat_date()
    a = agg_summary_data_by_day(stat_date)
    a()
