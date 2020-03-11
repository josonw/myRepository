#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGCluster, get_stat_date

class update_game_coin_type_dim(BasePGCluster):

    def stat(self):

        # 统一维表 增加dcbase库维表更新，2017-9-8
        sql = """
            INSERT INTO analysis.game_coin_type_dim (fgamefsk, fcointype, fdirection, ftype, fname)
            SELECT fgamefsk,
                   fcointype,
                   fdirection,
                   ftype,
                   ftype fname
            FROM
              (SELECT DISTINCT fgamefsk,
                               'GOODS' fcointype,
                                       CASE fdirection
                                           WHEN '1' THEN 'IN'
                                           WHEN '2' THEN 'OUT'
                                       END fdirection,
                                       ftype
               FROM dcnew.props_finace a
               WHERE fdate = date'%(ld_begin)s'
                 AND ftype IS NOT NULL) et
            WHERE NOT EXISTS
                (SELECT 1
                 FROM analysis.game_coin_type_dim edi
                 WHERE edi.fgamefsk = et.fgamefsk
                   AND edi.fcointype = et.fcointype
                   AND edi.fdirection = et.fdirection
                   AND edi.ftype = et.ftype)
            UNION all
            SELECT fgamefsk,
                   fcointype,
                   fdirection,
                   ftype,
                   ftype fname
            FROM
              (SELECT DISTINCT fgamefsk,
                               fcointype,
                               fdirection,
                               ftype
               FROM dcnew.gamecoin_detail a
               WHERE fdate = date'%(ld_begin)s'
                 AND ftype IS NOT NULL) et
            WHERE NOT EXISTS
                (SELECT 1
                 FROM analysis.game_coin_type_dim edi
                 WHERE edi.fgamefsk = et.fgamefsk
                   AND edi.fcointype = et.fcointype
                   AND edi.fdirection = et.fdirection
                   AND edi.ftype = et.ftype)
            UNION all
            SELECT fgamefsk,
                   fcointype,
                   fdirection,
                   ftype,
                   ftype fname
            FROM
              (SELECT DISTINCT fgamefsk,
                               fcointype,
                               fdirection,
                               ftype
               FROM dcnew.bycoin_general a
               WHERE fdate = date'%(ld_begin)s'
                 AND ftype IS NOT NULL) et
            WHERE NOT EXISTS
                (SELECT 1
                 FROM analysis.game_coin_type_dim edi
                 WHERE edi.fgamefsk = et.fgamefsk
                   AND edi.fcointype = et.fcointype
                   AND edi.fdirection = et.fdirection
                   AND edi.ftype = et.ftype)
            UNION all
            SELECT fgamefsk,
                   fcointype,
                   fdirection,
                   ftype,
                   ftype fname
            FROM
              (SELECT DISTINCT fgamefsk,
                               'GIFT' fcointype,
                                      fdirection,
                                      fgift_id ftype
               FROM dcnew.gift_finace a
               WHERE fdate = date'%(ld_begin)s'
                 AND fgift_id IS NOT NULL) et
            WHERE NOT EXISTS
                (SELECT 1
                 FROM analysis.game_coin_type_dim edi
                 WHERE edi.fgamefsk = et.fgamefsk
                   AND edi.fcointype = et.fcointype
                   AND edi.fdirection = et.fdirection
                   AND edi.ftype = et.ftype);

            INSERT INTO dcbase.game_coin_type_dim (fgamefsk, fcointype, fdirection, ftype, fname)
            SELECT fgamefsk,
                   fcointype,
                   fdirection,
                   ftype,
                   ftype fname
            FROM
              (SELECT DISTINCT fgamefsk,
                               'GOODS' fcointype,
                                       CASE fdirection
                                           WHEN '1' THEN 'IN'
                                           WHEN '2' THEN 'OUT'
                                       END fdirection,
                                       ftype
               FROM dcnew.props_finace a
               WHERE fdate = date'%(ld_begin)s'
                 AND ftype IS NOT NULL) et
            WHERE NOT EXISTS
                (SELECT 1
                 FROM dcbase.game_coin_type_dim edi
                 WHERE edi.fgamefsk = et.fgamefsk
                   AND edi.fcointype = et.fcointype
                   AND edi.fdirection = et.fdirection
                   AND edi.ftype = et.ftype)
            UNION all
            SELECT fgamefsk,
                   fcointype,
                   fdirection,
                   ftype,
                   ftype fname
            FROM
              (SELECT DISTINCT fgamefsk,
                               fcointype,
                               fdirection,
                               ftype
               FROM dcnew.gamecoin_detail a
               WHERE fdate = date'%(ld_begin)s'
                 AND ftype IS NOT NULL) et
            WHERE NOT EXISTS
                (SELECT 1
                 FROM dcbase.game_coin_type_dim edi
                 WHERE edi.fgamefsk = et.fgamefsk
                   AND edi.fcointype = et.fcointype
                   AND edi.fdirection = et.fdirection
                   AND edi.ftype = et.ftype)
            UNION all
            SELECT fgamefsk,
                   fcointype,
                   fdirection,
                   ftype,
                   ftype fname
            FROM
              (SELECT DISTINCT fgamefsk,
                               fcointype,
                               fdirection,
                               ftype
               FROM dcnew.bycoin_general a
               WHERE fdate = date'%(ld_begin)s'
                 AND ftype IS NOT NULL) et
            WHERE NOT EXISTS
                (SELECT 1
                 FROM dcbase.game_coin_type_dim edi
                 WHERE edi.fgamefsk = et.fgamefsk
                   AND edi.fcointype = et.fcointype
                   AND edi.fdirection = et.fdirection
                   AND edi.ftype = et.ftype)
            UNION all
            SELECT fgamefsk,
                   fcointype,
                   fdirection,
                   ftype,
                   ftype fname
            FROM
              (SELECT DISTINCT fgamefsk,
                               'GIFT' fcointype,
                                      fdirection,
                                      fgift_id ftype
               FROM dcnew.gift_finace a
               WHERE fdate = date'%(ld_begin)s'
                 AND fgift_id IS NOT NULL) et
            WHERE NOT EXISTS
                (SELECT 1
                 FROM dcbase.game_coin_type_dim edi
                 WHERE edi.fgamefsk = et.fgamefsk
                   AND edi.fcointype = et.fcointype
                   AND edi.fdirection = et.fdirection
                   AND edi.ftype = et.ftype);

        commit;
        """% self.sql_dict
        self.exe_hql(sql)

        sql = """
            UPDATE analysis.game_coin_type_dim a
            SET fdate_update =
              (SELECT fdate
               FROM
                 (SELECT DISTINCT fdate,
                                  fcointype,
                                  fgamefsk,
                                  fdirection,
                                  ftype
                  FROM dcnew.gamecoin_detail
                  WHERE fdate = date'%(ld_begin)s') b
               WHERE a.fgamefsk = b.fgamefsk
                 AND a.fdirection = b.fdirection
                 AND a.ftype = b.ftype)
            WHERE exists
                (SELECT 1
                 FROM
                   (SELECT DISTINCT fdate,fcointype, fgamefsk, fdirection, ftype
                    FROM dcnew.gamecoin_detail
                    WHERE fdate = date'%(ld_begin)s') edi
                 WHERE edi.fgamefsk = a.fgamefsk
                   AND edi.fcointype = a.fcointype
                   AND edi.fdirection = a.fdirection
                   AND edi.ftype = a.ftype);

            UPDATE dcbase.game_coin_type_dim a
            SET fdate_update =
              (SELECT fdate
               FROM
                 (SELECT DISTINCT fdate,
                                  fcointype,
                                  fgamefsk,
                                  fdirection,
                                  ftype
                  FROM dcnew.gamecoin_detail
                  WHERE fdate = date'%(ld_begin)s') b
               WHERE a.fgamefsk = b.fgamefsk
                 AND a.fdirection = b.fdirection
                 AND a.ftype = b.ftype)
            WHERE exists
                (SELECT 1
                 FROM
                   (SELECT DISTINCT fdate,fcointype, fgamefsk, fdirection, ftype
                    FROM dcnew.gamecoin_detail
                    WHERE fdate = date'%(ld_begin)s') edi
                 WHERE edi.fgamefsk = a.fgamefsk
                   AND edi.fcointype = a.fcointype
                   AND edi.fdirection = a.fdirection
                   AND edi.ftype = a.ftype);

        commit;
        """% self.sql_dict
        self.exe_hql(sql)

if __name__ == "__main__":
    stat_date = get_stat_date()
    a = update_game_coin_type_dim(stat_date)
    a()
