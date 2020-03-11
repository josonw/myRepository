#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date

class update_game_coin_type_dim(BasePGStat):

    def stat(self):

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
               FROM analysis.props_finace_fct a
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
               FROM analysis.pay_game_coin_finace_fct a
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
               FROM analysis.user_bycoin_stream_fct a
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
                                      ftype
               FROM analysis.gift_finace_fct a
               WHERE fdate = date'%(ld_begin)s'
                 AND ftype IS NOT NULL) et
            WHERE NOT EXISTS
                (SELECT 1
                 FROM analysis.game_coin_type_dim edi
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
                  FROM analysis.pay_game_coin_finace_fct
                  WHERE fdate = date'%(ld_begin)s') b
               WHERE a.fgamefsk = b.fgamefsk
                 AND a.fdirection = b.fdirection
                 AND a.ftype = b.ftype)
            WHERE exists
                (SELECT 1
                 FROM
                   (SELECT DISTINCT fdate,fcointype, fgamefsk, fdirection, ftype
                    FROM pay_game_coin_finace_fct
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
