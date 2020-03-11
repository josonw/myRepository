#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from BaseStat import BasePGStat, get_stat_date


class ReloadDimFif(BasePGStat):
    """bud相关的所有维表的更新SQL集合
    """

    def stat(self):
        sql = """
            -- 游戏货币类型
            INSERT INTO dcbase.game_coin_type_dim(fgamefsk, fcointype, fdirection, ftype, fname, fdate_update)
            SELECT
              fgamefsk,
              fcointype,
              fdirection,
              ftype,
              ftype fname,
              now() fdate_update
            FROM
              (SELECT DISTINCT
                fgamefsk,
                'GOODS' fcointype,
                CASE fdirection
                    WHEN '1' THEN 'IN'
                    WHEN '2' THEN 'OUT'
                END fdirection,
                ftype
              FROM dcnew.props_finace a
              WHERE fdate = date'%(ld_begin)s'
                AND ftype IS NOT NULL) et
            WHERE NOT EXISTS(
              SELECT 1
              FROM dcbase.game_coin_type_dim edi
              WHERE edi.fgamefsk = et.fgamefsk
                AND edi.fcointype = et.fcointype
                AND edi.fdirection = et.fdirection
                AND edi.ftype = et.ftype)
            UNION all
            SELECT
              fgamefsk,
              fcointype,
              fdirection,
              ftype,
              ftype fname,
              now() fdate_update
            FROM
              (SELECT DISTINCT
                fgamefsk,
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
            SELECT
              fgamefsk,
              fcointype,
              fdirection,
              ftype,
              ftype fname,
              now() fdate_update
            FROM(
              SELECT DISTINCT fgamefsk,
                fcointype,
                fdirection,
                ftype
              FROM dcnew.bycoin_general a
              WHERE fdate = date'%(ld_begin)s'
                AND ftype IS NOT NULL
            ) et
            WHERE NOT EXISTS(
              SELECT 1
              FROM dcbase.game_coin_type_dim edi
              WHERE edi.fgamefsk = et.fgamefsk
                AND edi.fcointype = et.fcointype
                AND edi.fdirection = et.fdirection
                AND edi.ftype = et.ftype)
            UNION all
            SELECT
              fgamefsk,
              fcointype,
              fdirection,
              ftype,
              ftype fname,
              now() fdate_update
            FROM(
              SELECT DISTINCT
                fgamefsk,
                'GIFT' fcointype,
                fdirection,
                fgift_id ftype
              FROM dcnew.gift_finace a
              WHERE fdate = date'%(ld_begin)s'
                AND fgift_id IS NOT NULL) et
            WHERE NOT EXISTS(
              SELECT 1
              FROM dcbase.game_coin_type_dim edi
              WHERE edi.fgamefsk = et.fgamefsk
                AND edi.fcointype = et.fcointype
                AND edi.fdirection = et.fdirection
                AND edi.ftype = et.ftype
            );
        """ % self.sql_dict

        try:
            self.exe_hql(sql)
        except Exception as e:
            print('游戏货币类型维表报错')
            print(e)

        sql = """
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
        """ % self.sql_dict

        try:
            self.exe_hql(sql)
        except Exception as e:
            print('游戏货币类型维表更新报错')
            print(e)

        sql = """
            INSERT INTO dcbase.game_activity_rule_dim(fgamefsk, fact_id, frule_id, fdis_name, fdate_update)
            SELECT a.fgamefsk, a.fact_id, a.frule_id, a.frule_id fdis_name, now() fdate_update
            FROM analysis.game_activity_rule_fct a
            LEFT OUTER JOIN dcbase.game_activity_rule_dim b
              ON a.fgamefsk = b.fgamefsk
              and a.fact_id = b.fact_id
              and a.frule_id = b.frule_id
            WHERE a.fdate = '%(ld_begin)s'
              and b.frule_id is null
            GROUP BY
              a.fgamefsk, a.fact_id, a.frule_id;
        """ % self.sql_dict

        try:
            self.exe_hql(sql)
        except Exception as e:
            print('game_activity_rule_dim表格更新报错')
            print(e)

        sql = """
            INSERT INTO dcbase.game_activity_dim(fgamefsk,fact_id,fact_name,fdis_name, fdate_update)
            SELECT a.fgamefsk, a.fact_id, max(a.fact_name) fact_name, max(a.fact_name) fdis_name, now() fdate_update
            FROM analysis.game_activity_fct a
            LEFT OUTER JOIN dcbase.game_activity_dim b
              ON a.fgamefsk=b.fgamefsk
              and a.fact_id=b.fact_id
            WHERE a.fdate = '%(ld_begin)s'
              and b.fact_id is null
            GROUP BY
              a.fgamefsk, a.fact_id;
        """ % self.sql_dict

        try:
            self.exe_hql(sql)
        except Exception as e:
            print('game_activity_dim表格更新报错')
            print(e)

        sql = """
            DELETE FROM dcbase.event_dim where fdate = '%(ld_begin)s';
            INSERT INTO dcbase.event_dim (fdate, fgamefsk, fet_id, fet_name, fdate_update)
            SELECT '%(ld_begin)s' fdate,
              fgamefsk,
              fet_id,
              fet_id fet_name,
              now() fdate_update
            FROM (
              SELECT
                distinct fgamefsk,fet_id
              FROM dcnew.event a
              WHERE fdate = '%(ld_begin)s'
                and fet_id is not null
            ) a
            WHERE NOT EXISTS (
              SELECT 1
              FROM dcbase.event_dim be
              WHERE be.fgamefsk = a.fgamefsk
              and be.fet_id = a.fet_id);
        """ % self.sql_dict

        try:
            self.exe_hql(sql)
        except Exception as e:
            print('事件指标维度更新报错')
            print(e)

        sql = """
            -- 虚拟货币(游戏币、博雅币)配置
            INSERT INTO dcbase.currencies_type_dim (fgamefsk, fcointype, fact_type, fact_id, fname, fdate_update)
            SELECT
              fgamefsk,
              fcointype,
              fact_type,
              fact_id,
              fact_id fname,
              now() fdate_update
            FROM (
              SELECT DISTINCT fgamefsk,fcointype,fact_type,fact_id
              FROM dcnew.currencies_detail a
              WHERE fdate = '%(ld_begin)s'
                and fcointype is not null ) a
            WHERE NOT EXISTS (
              SELECT 1
              FROM dcbase.currencies_type_dim be
              WHERE be.fgamefsk = a.fgamefsk
                and be.fcointype = a.fcointype
                and be.fact_type = a.fact_type
                and be.fact_id = a.fact_id
            );
        """ % self.sql_dict

        try:
            self.exe_hql(sql)
        except Exception as e:
            print('虚拟货币(游戏币、博雅币)配置')
            print(e)

        sql = """
            --  子游戏维表
            INSERT INTO dcbase.subgame_dim (fsubgamefsk,fsubgamename,fgamefsk,priority, fdate_update)
            SELECT
              fsubgamefsk,
              fsubgamefsk,
              fgamefsk,
              0 priority,
              now() fdate_update
            FROM (
              SELECT DISTINCT fsubgamefsk,fgamefsk
              FROM dcnew.act_user a
              WHERE fdate = '%(ld_begin)s'
                AND fsubgamefsk <> -21379
                AND fsubgamefsk <> -13658
            ) a
            WHERE NOT EXISTS ( SELECT 1
              FROM dcbase.subgame_dim be
              WHERE be.fgamefsk = a.fgamefsk
                and be.fsubgamefsk = a.fsubgamefsk);
        """ % self.sql_dict

        try:
            self.exe_hql(sql)
        except Exception as e:
            print('子游戏维表配置')
            print(e)

        sql = """
            -- 二级场次维表
            INSERT INTO dcbase.gameparty_name_dim(fgamefsk, fgameparty_name, fdis_name, fdate_update)
            SELECT
              fgamefsk,
              fsubname,
              fsubname,
              now() fdate_update
            FROM(
              SELECT DISTINCT
                fgamefsk,
                fsubname
              FROM dcnew.gameparty_settlement a
              WHERE fdate >= date'%(ld_begin)s'
                AND fdate < date'%(ld_end)s'
                AND fsubname IS NOT NULL
              ) a
            WHERE NOT EXISTS(
              SELECT 1
              FROM dcbase.gameparty_name_dim be
              WHERE be.fgamefsk = a.fgamefsk
                AND be.fgameparty_name = a.fsubname
            );
        """ % self.sql_dict

        try:
            self.exe_hql(sql)
        except Exception as e:
            print('子游戏维表配置')
            print(e)

        sql = """
            -- 未知
            INSERT INTO dcbase.payment_channel_dim_cfg(fm_fsk,fm_id,fm_name,fmobilename,fpay_kind, fdate_update)
            SELECT
              fm_id, fm_id, fm_name, '其他', '其他', now() fdate_update
            FROM(
              SELECT DISTINCT
                fm_id, fm_name
              FROM dcbase.payment_channel_dim a
              WHERE fm_id IS NOT NULL
                AND fm_name IS NOT NULL
            ) a
            WHERE NOT EXISTS(
              SELECT 1
              FROM dcbase.payment_channel_dim_cfg be
              WHERE be.fm_id = a.fm_id
            );
        """ % self.sql_dict

        try:
            self.exe_hql(sql)
        except Exception as e:
            print('dcbase.payment_channel_dim_cfg表格报错')
            print(e)

        sql = """
            -- 更新bpid和游戏，平台的映射
            UPDATE dcbase.bpid_platform_game_ver_map s
            SET fdip=(
              SELECT f30dincome
              FROM analysis.user_payment_fct a
              WHERE a.fdate >= date'%(ld_begin)s'
                AND a.fdate < date'%(ld_end)s'
                AND a.fgamefsk=s.fgamefsk
                AND a.fplatformfsk=s.fplatformfsk
                AND a.fversionfsk=s.fversionfsk
            ), fdate_update = now();

            -- 更新bpid和游戏，平台的映射
            UPDATE dcbase.bpid_platform_game_ver_map
            SET fdip =0, fdate_update = now()
            WHERE fdip IS NULL;

            -- 更新bpid和游戏，平台的映射
            UPDATE dcbase.bpid_platform_game_ver_map s
            SET fdau = (
              SELECT factcnt
              FROM analysis.user_true_active_fct a
              WHERE a.fdate >= date'%(ld_begin)s'
                AND a.fdate < date'%(ld_end)s'
                AND a.fgamefsk=s.fgamefsk
                AND a.fplatformfsk=s.fplatformfsk
                AND a.fversionfsk=s.fversionfsk
            ), fdate_update = now();

            -- 更新bpid和游戏，平台的映射
            UPDATE dcbase.bpid_platform_game_ver_map
            SET fdau  =0, fdate_update = now()
            WHERE fdau  IS NULL;
        """ % self.sql_dict

        try:
            self.exe_hql(sql)
        except Exception as e:
            print('更新bpid和游戏，平台的映射报错')
            print(e)

        sql = """
          INSERT INTO dcbase.ad_dim(ad_name, ad_id, fgamefsk)
          SELECT fuser_type_id ad_name, fuser_type_id ad_id, fgamefsk
          FROM(
            select distinct fuser_type_id, fgamefsk from dcnew.xxx_user_info a
            WHERE a.fdate >= date'%(ld_begin)s'
              AND a.fdate < date'%(ld_end)s'
          ) a
          WHERE(fuser_type_id, fgamefsk) NOT IN (
            select fuser_type_id, fgamefsk from dcbase.ad_dim b where a.fuser_type_id = b.ad_id
          )
        """ % self.sql_dict

        try:
            self.exe_hql(sql)
        except Exception as e:
            print('广告维表更新失败')
            print(e)


if __name__ == "__main__":

    stat_date = get_stat_date()
    # 生成统计实例
    a = ReloadDimFif(stat_date)
    a()
