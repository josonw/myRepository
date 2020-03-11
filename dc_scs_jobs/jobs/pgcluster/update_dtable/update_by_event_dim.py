#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date

class update_by_event_dim(BasePGCluster):

    def stat(self):
        # pg集群legacy optimizer不支持多行更新
        # 统一维表 增加dcbase库维表更新，2017-9-8
        sql = """set optimizer=off;"""
        self.exe_hql(sql)


        sql = """

                INSERT INTO analysis.by_event_dim (fgamefsk, fet_id, fet_name)
                SELECT fgamefsk,
                       fet_id,
                       fet_id
                FROM
                  ( SELECT DISTINCT fgamefsk,
                                    fet_id
                   FROM analysis.by_event_fct a
                   WHERE fdate >= date'%(ld_begin)s'
                     AND fdate < date'%(ld_end)s'
                     AND fet_id IS NOT NULL ) a
                WHERE NOT EXISTS
                    ( SELECT 1
                     FROM analysis.by_event_dim be
                     WHERE be.fgamefsk = a.fgamefsk
                       AND be.fet_id = a.fet_id );

        commit;
        """% self.sql_dict
        self.exe_hql(sql)


        sql = """

                INSERT INTO dcnew.gameparty_name_dim (fgamefsk, fgameparty_name, fdis_name)
                SELECT fgamefsk,
                       fsubname,
                       fsubname
                FROM
                  ( SELECT DISTINCT fgamefsk,
                                    fsubname
                   FROM dcnew.gameparty_settlement a
                   WHERE fdate >= date'%(ld_begin)s'
                     AND fdate < date'%(ld_end)s'
                     AND fsubname IS NOT NULL ) a
                WHERE NOT EXISTS
                    ( SELECT 1
                     FROM dcnew.gameparty_name_dim be
                     WHERE be.fgamefsk = a.fgamefsk
                       AND be.fgameparty_name = a.fsubname );

                INSERT INTO dcbase.gameparty_name_dim (fgamefsk, fgameparty_name, fdis_name)
                SELECT fgamefsk,
                       fsubname,
                       fsubname
                FROM
                  ( SELECT DISTINCT fgamefsk,
                                    fsubname
                   FROM dcnew.gameparty_settlement a
                   WHERE fdate >= date'%(ld_begin)s'
                     AND fdate < date'%(ld_end)s'
                     AND fsubname IS NOT NULL ) a
                WHERE NOT EXISTS
                    ( SELECT 1
                     FROM dcbase.gameparty_name_dim be
                     WHERE be.fgamefsk = a.fgamefsk
                       AND be.fgameparty_name = a.fsubname );

        commit;
        """% self.sql_dict
        self.exe_hql(sql)



        sql = """
                INSERT INTO analysis.payment_channel_dim_cfg
                (fm_fsk,fm_id,fm_name,fmobilename,fpay_kind)
                SELECT fm_id,
                       fm_id,
                       fm_name,
                       '其他',
                       '其他'
                FROM
                  (SELECT DISTINCT fm_id,
                                   fm_name
                   FROM analysis.payment_channel_dim a
                   WHERE fm_id IS NOT NULL
                     AND fm_name IS NOT NULL) a
                WHERE NOT EXISTS
                    (SELECT 1
                     FROM analysis.payment_channel_dim_cfg be
                     WHERE be.fm_id = a.fm_id
                       );

                INSERT INTO dcbase.payment_channel_dim_cfg
                (fm_fsk,fm_id,fm_name,fmobilename,fpay_kind)
                SELECT fm_id,
                       fm_id,
                       fm_name,
                       '其他',
                       '其他'
                FROM
                  (SELECT DISTINCT fm_id,
                                   fm_name
                   FROM dcbase.payment_channel_dim a
                   WHERE fm_id IS NOT NULL
                     AND fm_name IS NOT NULL) a
                WHERE NOT EXISTS
                    (SELECT 1
                     FROM dcbase.payment_channel_dim_cfg be
                     WHERE be.fm_id = a.fm_id
                       );

                commit;
        """% self.sql_dict
        self.exe_hql(sql)

        sql = """
                UPDATE analysis.payment_channel_dim s
                SET fm_name = c.pmodename
                FROM analysis.paycenter_chanel_dim c
                WHERE c.statid = s.fm_id::int
                and c.is_use = 0;

                UPDATE dcbase.payment_channel_dim s
                SET fm_name = c.pmodename
                FROM dcbase.paycenter_chanel_dim c
                WHERE c.statid = s.fm_id::int
                and c.is_use = 0;

                commit;
        """% self.sql_dict
        self.exe_hql(sql)

        sql = """
                UPDATE analysis.payment_channel_dim_cfg s
                SET fm_name = c.fm_name
                FROM analysis.payment_channel_dim c
                WHERE c.fm_id = s.fm_id;

                UPDATE dcbase.payment_channel_dim_cfg s
                SET fm_name = c.fm_name
                FROM dcbase.payment_channel_dim c
                WHERE c.fm_id = s.fm_id;

                commit;
        """% self.sql_dict
        self.exe_hql(sql)

        sql = """
            UPDATE analysis.bpid_platform_game_ver_map s
            SET fdip=
              (SELECT f30dincome
               FROM analysis.user_payment_fct a
               WHERE a.fdate >= date'%(ld_begin)s'
                 AND a.fdate < date'%(ld_end)s'
                 AND a.fgamefsk=s.fgamefsk
                 AND a.fplatformfsk=s.fplatformfsk
                 AND a.fversionfsk=s.fversionfsk);

            UPDATE dcbase.bpid_platform_game_ver_map s
            SET fdip=
              (SELECT f30dincome
               FROM analysis.user_payment_fct a
               WHERE a.fdate >= date'%(ld_begin)s'
                 AND a.fdate < date'%(ld_end)s'
                 AND a.fgamefsk=s.fgamefsk
                 AND a.fplatformfsk=s.fplatformfsk
                 AND a.fversionfsk=s.fversionfsk);

            COMMIT;
        """% self.sql_dict
        self.exe_hql(sql)

        sql = """
            UPDATE dcbase.bpid_platform_game_ver_map
            SET fdip =0
            WHERE fdip IS NULL;

            UPDATE dcbase.bpid_platform_game_ver_map
            SET fdip =0
            WHERE fdip IS NULL;

            COMMIT;
        """% self.sql_dict
        self.exe_hql(sql)

        sql = """
            UPDATE analysis.bpid_platform_game_ver_map s
            SET fdau =
              (SELECT factcnt
               FROM analysis.user_true_active_fct a
               WHERE a.fdate >= date'%(ld_begin)s'
                 AND a.fdate < date'%(ld_end)s'
                 AND a.fgamefsk=s.fgamefsk
                 AND a.fplatformfsk=s.fplatformfsk
                 AND a.fversionfsk=s.fversionfsk);

            UPDATE dcbase.bpid_platform_game_ver_map s
            SET fdau =
              (SELECT factcnt
               FROM analysis.user_true_active_fct a
               WHERE a.fdate >= date'%(ld_begin)s'
                 AND a.fdate < date'%(ld_end)s'
                 AND a.fgamefsk=s.fgamefsk
                 AND a.fplatformfsk=s.fplatformfsk
                 AND a.fversionfsk=s.fversionfsk);

            COMMIT;
        """% self.sql_dict
        self.exe_hql(sql)


        sql = """
            UPDATE dcbase.bpid_platform_game_ver_map
            SET fdau  =0
            WHERE fdau  IS NULL;

            UPDATE dcbase.bpid_platform_game_ver_map
            SET fdau  =0
            WHERE fdau  IS NULL;

            COMMIT;
        """% self.sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":
    stat_date = get_stat_date()
    a = update_by_event_dim(stat_date)
    a()
