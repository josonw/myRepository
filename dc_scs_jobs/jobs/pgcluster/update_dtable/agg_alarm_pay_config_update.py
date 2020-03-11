#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
import collections
from dateutil.relativedelta import *
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStat import BasePGCluster, get_stat_date, pg_db

class agg_alarm_pay_config_update(BasePGCluster):
    def stat(self):
        sql = """INSERT INTO analysis.alarm_pay_config(fgamefsk, fgamename, fpay_kind, fmobilename, fm_id, fm_name, fcountry, fprovince, fdim, fdim_cn, fdays_pre, fdim_pre, updown_pre, fvalue_pre, fvalue_tb, fvalue_hb, onoff)

            SELECT a.fgamefsk,
                   gd.fname fgamename,
                   pc.fpay_kind,
                   pc.fmobilename,
                   a.fm_id,
                   pc.fm_name,
                   a.fcountry,
                   a.fprovince,
                   'fmoney' AS fdim,
                   '付费金额' AS fdim_cn,
                   5,
                   'fmoney' AS fdim_pre,
                   '>=' AS updown_pre,
                   100,
                   10,
                   10,
                   1
            FROM analysis.pay_channel_province_fct a
            LEFT JOIN analysis.game_dim gd
            ON a.fgamefsk = gd.fsk
            JOIN analysis.payment_channel_dim_cfg pc
            ON a.fm_id =pc.fm_id
            LEFT OUTER JOIN analysis.alarm_pay_config b
            ON a.fgamefsk=b.fgamefsk
            AND a.fm_id=b.fm_id
            and a.fprovince = b.fprovince
            and b.fgamefsk!=0
            WHERE a.fdate = '%(ld_begin)s'
              AND b.fgamefsk IS NULL
              and pc.fpay_kind = '短信'
              and a.fcountry = '中国'
            GROUP BY a.fgamefsk,
                   gd.fname,
                   pc.fpay_kind,
                   pc.fmobilename,
                   a.fm_id,
                   pc.fm_name,
                   a.fcountry,
                   a.fprovince
            union

            SELECT 0 as fgamefsk,
                   '所有游戏' as fgamename,
                   pc.fpay_kind,
                   pc.fmobilename,
                   a.fm_id,
                   pc.fm_name,
                   a.fcountry,
                   a.fprovince,
                   'fmoney' AS fdim,
                   '付费金额' AS fdim_cn,
                   5,
                   'fmoney' AS fdim_pre,
                   '>=' AS updown_pre,
                   500,
                   10,
                   10,
                   1
            FROM analysis.pay_channel_province_fct a
            JOIN analysis.payment_channel_dim_cfg pc
            ON a.fm_id =pc.fm_id
            LEFT OUTER JOIN analysis.alarm_pay_config b
            ON  a.fm_id=b.fm_id
            and a.fprovince = b.fprovince
            and b.fgamefsk = 0
            WHERE a.fdate = '%(ld_begin)s'
              AND b.fgamefsk IS NULL
              and pc.fpay_kind = '短信'
              and a.fcountry = '中国'
            GROUP BY pc.fpay_kind,
                   pc.fmobilename,
                   a.fm_id,
                   pc.fm_name,
                   a.fcountry,
                   a.fprovince
           union
           SELECT a.fgamefsk,
                   gd.fname fgamename,
                   pc.fpay_kind,
                   pc.fmobilename,
                   a.fm_id,
                   pc.fm_name,
                   a.fcountry,
                   a.fprovince,
                   'forder' AS fdim,
                   '订单数' AS fdim_cn,
                   5,
                   'fmoney' AS fdim_pre,
                   '>=' AS updown_pre,
                   100,
                   10,
                   10,
                   1
            FROM analysis.pay_channel_province_fct a
            LEFT JOIN analysis.game_dim gd
            ON a.fgamefsk = gd.fsk
            JOIN analysis.payment_channel_dim_cfg pc
            ON a.fm_id =pc.fm_id
            LEFT OUTER JOIN analysis.alarm_pay_config b
            ON a.fgamefsk=b.fgamefsk
            AND a.fm_id=b.fm_id
            and a.fprovince = b.fprovince
            and b.fgamefsk!=0
            WHERE a.fdate = '%(ld_begin)s'
              AND b.fgamefsk IS NULL
              and pc.fpay_kind = '短信'
              and a.fcountry = '中国'
            GROUP BY a.fgamefsk,
                   gd.fname,
                   pc.fpay_kind,
                   pc.fmobilename,
                   a.fm_id,
                   pc.fm_name,
                   a.fcountry,
                   a.fprovince
            union

            SELECT 0 as fgamefsk,
                   '所有游戏' as fgamename,
                   pc.fpay_kind,
                   pc.fmobilename,
                   a.fm_id,
                   pc.fm_name,
                   a.fcountry,
                   a.fprovince,
                   'forder' AS fdim,
                   '订单数' AS fdim_cn,
                   5,
                   'fmoney' AS fdim_pre,
                   '>=' AS updown_pre,
                   500,
                   10,
                   10,
                   1
            FROM analysis.pay_channel_province_fct a
            JOIN analysis.payment_channel_dim_cfg pc
            ON a.fm_id =pc.fm_id
            LEFT OUTER JOIN analysis.alarm_pay_config b
            ON  a.fm_id=b.fm_id
            and a.fprovince = b.fprovince
            and b.fgamefsk = 0
            WHERE a.fdate = '%(ld_begin)s'
              AND b.fgamefsk IS NULL
              and pc.fpay_kind = '短信'
              and a.fcountry = '中国'
            GROUP BY pc.fpay_kind,
                   pc.fmobilename,
                   a.fm_id,
                   pc.fm_name,
                   a.fcountry,
                   a.fprovince
        union
        SELECT a.fgamefsk,
                   gd.fname fgamename,
                   pc.fpay_kind,
                   pc.fmobilename,
                   a.fm_id,
                   pc.fm_name,
                   a.fcountry,
                   a.fprovince,
                   'ocr' AS fdim,
                   '订单完成率' AS fdim_cn,
                   5,
                   'fmoney' AS fdim_pre,
                   '>=' AS updown_pre,
                   100,
                   10,
                   10,
                   1
            FROM analysis.pay_channel_province_fct a
            LEFT JOIN analysis.game_dim gd
            ON a.fgamefsk = gd.fsk
            JOIN analysis.payment_channel_dim_cfg pc
            ON a.fm_id =pc.fm_id
            LEFT OUTER JOIN analysis.alarm_pay_config b
            ON a.fgamefsk=b.fgamefsk
            AND a.fm_id=b.fm_id
            and a.fprovince = b.fprovince
            and b.fgamefsk!=0
            WHERE a.fdate = '%(ld_begin)s'
              AND b.fgamefsk IS NULL
              and pc.fpay_kind = '短信'
              and a.fcountry = '中国'
            GROUP BY a.fgamefsk,
                   gd.fname,
                   pc.fpay_kind,
                   pc.fmobilename,
                   a.fm_id,
                   pc.fm_name,
                   a.fcountry,
                   a.fprovince
            union

            SELECT 0 as fgamefsk,
                   '所有游戏' as fgamename,
                   pc.fpay_kind,
                   pc.fmobilename,
                   a.fm_id,
                   pc.fm_name,
                   a.fcountry,
                   a.fprovince,
                   'ocr' AS fdim,
                   '订单完成率' AS fdim_cn,
                   5,
                   'fmoney' AS fdim_pre,
                   '>=' AS updown_pre,
                   500,
                   10,
                   10,
                   1
            FROM analysis.pay_channel_province_fct a
            JOIN analysis.payment_channel_dim_cfg pc
            ON a.fm_id =pc.fm_id
            LEFT OUTER JOIN analysis.alarm_pay_config b
            ON  a.fm_id=b.fm_id
            and a.fprovince = b.fprovince
            and b.fgamefsk = 0
            WHERE a.fdate = '%(ld_begin)s'
              AND b.fgamefsk IS NULL
              and pc.fpay_kind = '短信'
              and a.fcountry = '中国'
            GROUP BY pc.fpay_kind,
                   pc.fmobilename,
                   a.fm_id,
                   pc.fm_name,
                   a.fcountry,
                   a.fprovince ;
             COMMIT;
        """% self.sql_dict
        self.exe_hql(sql)


if __name__ == "__main__":
    stat_date = get_stat_date()
    a = agg_alarm_pay_config_update(stat_date)
    a()
