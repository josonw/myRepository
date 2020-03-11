#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserCoinSource(BaseStatModel):
    def create_tab(self):
        hql = """
            create table if not exists stage_dfqp.user_coin_type_daily
            (
              fuid bigint comment '用户ID',
              total_coin bigint comment '游戏币总量（金条按1:100转银币）',
              coin_type string comment '货币类型',
              coin_num bigint comment '货币数量',
              coin_percent bigint comment '货币类型占比',
              fbpid string comment 'BPID',
              fgamefsk bigint COMMENT '游戏ID',
              fgamename string COMMENT '游戏名称',
              fplatformfsk bigint COMMENT '平台ID',
              fplatformname string COMMENT '平台名称',
              fhallfsk bigint COMMENT '大厅ID',
              fhallname string COMMENT '大厅名称',
              fterminaltypefsk bigint COMMENT '终端ID',
              fterminaltypename string COMMENT '终端名称',
              fversionfsk bigint COMMENT '版本ID',
              fversionname string COMMENT '版本名称'
            )
            comment '用户每日获得游戏币'
            partitioned by (dt string comment '日期')
            stored as parquet
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            create table if not exists stage_dfqp.user_coin_source_daily
            (
              fuid bigint comment '用户ID',
              coin_type string comment '货币类型',
              coin_source string comment '货币来源',
              coin_num bigint comment '货币数量',
              coin_percent bigint comment '货币来源占比',
              fbpid string comment 'BPID',
              fgamefsk bigint COMMENT '游戏ID',
              fgamename string COMMENT '游戏名称',
              fplatformfsk bigint COMMENT '平台ID',
              fplatformname string COMMENT '平台名称',
              fhallfsk bigint COMMENT '大厅ID',
              fhallname string COMMENT '大厅名称',
              fterminaltypefsk bigint COMMENT '终端ID',
              fterminaltypename string COMMENT '终端名称',
              fversionfsk bigint COMMENT '版本ID',
              fversionname string COMMENT '版本名称'
            )
            comment '用户游戏币来源大类'
            partitioned by (dt string comment '日期')
            stored as parquet
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            create table if not exists stage_dfqp.user_coin_source_subclass_daily
            (
              fuid bigint comment '用户ID',
              coin_type string comment '货币类型',
              coin_source string comment '货币来源大类',
              coin_source_subclass string comment '货币来源小类',
              coin_num bigint comment '货币数量',
              coin_percent bigint comment '货币来源小类占比',
              fbpid string comment 'BPID',
              fgamefsk bigint COMMENT '游戏ID',
              fgamename string COMMENT '游戏名称',
              fplatformfsk bigint COMMENT '平台ID',
              fplatformname string COMMENT '平台名称',
              fhallfsk bigint COMMENT '大厅ID',
              fhallname string COMMENT '大厅名称',
              fterminaltypefsk bigint COMMENT '终端ID',
              fterminaltypename string COMMENT '终端名称',
              fversionfsk bigint COMMENT '版本ID',
              fversionname string COMMENT '版本名称'
            )
            comment '用户游戏币来源小类'
            partitioned by (dt string comment '日期')
            stored as parquet
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            create table if not exists stage_dfqp.user_coin_scene_lv1_daily
            (
              fuid bigint comment '用户ID',
              coin_type string comment '货币类型',
              coin_source string comment '货币来源大类',
              coin_source_subclass string comment '货币来源小类',
              coin_scene_lv1 string comment '货币来源一级场次',
              coin_num bigint comment '货币数量',
              coin_percent bigint comment '货币来源一级场占比',
              fbpid string comment 'BPID',
              fgamefsk bigint COMMENT '游戏ID',
              fgamename string COMMENT '游戏名称',
              fplatformfsk bigint COMMENT '平台ID',
              fplatformname string COMMENT '平台名称',
              fhallfsk bigint COMMENT '大厅ID',
              fhallname string COMMENT '大厅名称',
              fterminaltypefsk bigint COMMENT '终端ID',
              fterminaltypename string COMMENT '终端名称',
              fversionfsk bigint COMMENT '版本ID',
              fversionname string COMMENT '版本名称'
            )
            comment '用户游戏币来源一级场细则'
            partitioned by (dt string comment '日期')
            stored as parquet
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            create table if not exists stage_dfqp.user_coin_scene_lv2_daily
            (
              fuid bigint comment '用户ID',
              coin_type string comment '货币类型',
              coin_source string comment '货币来源大类',
              coin_source_subclass string comment '货币来源小类',
              coin_scene_lv1 string comment '货币来源一级场次',
              coin_scene_lv2 string comment '货币来源二级场次',
              coin_num bigint comment '货币数量',
              coin_percent bigint comment '货币来源二级场占比',
              fbpid string comment 'BPID',
              fgamefsk bigint COMMENT '游戏ID',
              fgamename string COMMENT '游戏名称',
              fplatformfsk bigint COMMENT '平台ID',
              fplatformname string COMMENT '平台名称',
              fhallfsk bigint COMMENT '大厅ID',
              fhallname string COMMENT '大厅名称',
              fterminaltypefsk bigint COMMENT '终端ID',
              fterminaltypename string COMMENT '终端名称',
              fversionfsk bigint COMMENT '版本ID',
              fversionname string COMMENT '版本名称'
            )
            comment '用户游戏币来源二级场细则'
            partitioned by (dt string comment '日期')
            stored as parquet
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
            create table if not exists stage_dfqp.user_coin_source_tag
            (
              fuid bigint comment '用户ID',
              pay_sum decimal(20,2) comment '付费额',
              total_coin bigint comment '游戏币总量（金条按1:100转银币）',
              coin_type string comment '货币类型',
              coin_type_num bigint comment '货币类型额度',
              coin_type_percent bigint comment '货币类型百分比',
              coin_source string comment '货币来源大类',
              coin_source_num bigint comment '货币来源大类额度',
              coin_source_percent bigint comment '货币来源大类百分比',
              coin_source_subclass string comment '货币来源小类',
              coin_source_subclass_num bigint comment '货币来源小类额度',
              coin_source_subclass_percent bigint comment '货币来源小类百分比',
              coin_scene_lv1 string comment '货币来源一级场次',
              coin_scene_lv1_num bigint comment '货币来源一级场额度',
              coin_scene_lv1_percent bigint comment '货币来源一级场百分比',
              coin_scene_lv2 string comment '货币来源二级场次',
              coin_scene_lv2_num bigint comment '货币来源二级场额度',
              coin_scene_lv2_percent bigint comment '货币来源二级场百分比',
              fbpid string comment 'BPID',
              fgamefsk bigint comment '游戏ID',
              fgamename string comment '游戏名称',
              fplatformfsk bigint comment '平台ID',
              fplatformname string comment '平台名称',
              fhallfsk bigint comment '大厅ID',
              fhallname string comment '大厅名称',
              fterminaltypefsk bigint comment '终端ID',
              fterminaltypename string comment '终端名称',
              fversionfsk bigint comment '版本ID',
              fversionname string comment '版本名称'
            )
            comment '用户游戏币主要来源'
            partitioned by (dt string comment '日期')
            stored as parquet
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

    def stat(self):
        HQL_coin_source_subclass = """
            with pre_data as
             (select fuid,
                     cast('银币' as string) as coin_type,
                     case
                       when fact_id in ('105', '106', '122', '128') then
                        '付费'
                       when fact_id in ('101', '139', '2', '114', '120', '102', '131') then
                        '系统产出'
                       when fact_id in ('138') then
                        '兑换'
                       else
                        null
                     end as coin_source,
                     case
                       when fact_id in ('105', '106', '122', '128') then
                        '付费'
                       when fact_id in ('101', '139') then
                        '签到'
                       when fact_id in ('2') then
                        '救济'
                       when fact_id in ('114', '120') then
                        '活动'
                       when fact_id in ('102', '131') then
                        '特殊'
                       when fact_id in ('138') then
                        '金条'
                       else
                        null
                     end as coin_source_subclass,
                     sum(abs(fact_num)) as coin_num,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from stage_dfqp.pb_gamecoins_stream_stg
               where fact_type = 1
                 and fact_id in ('105',
                                 '106',
                                 '122',
                                 '128',
                                 '101',
                                 '139',
                                 '2',
                                 '114',
                                 '120',
                                 '102',
                                 '131',
                                 '138')
                 and dt = '%(statdate)s'
               group by fuid,
                        case
                          when fact_id in ('105', '106', '122', '128') then
                           '付费'
                          when fact_id in ('101', '139', '2', '114', '120', '102', '131') then
                           '系统产出'
                          when fact_id in ('138') then
                           '兑换'
                          else
                           null
                        end,
                        case
                          when fact_id in ('105', '106', '122', '128') then
                           '付费'
                          when fact_id in ('101', '139') then
                           '签到'
                          when fact_id in ('2') then
                           '救济'
                          when fact_id in ('114', '120') then
                           '活动'
                          when fact_id in ('102', '131') then
                           '特殊'
                          when fact_id in ('138') then
                           '金条'
                          else
                           null
                        end,
                        fbpid,
                        fgamefsk,
                        fgamename,
                        fplatformfsk,
                        fplatformname,
                        fhallfsk,
                        fhallname,
                        fterminaltypefsk,
                        fterminaltypename,
                        fversionfsk,
                        fversionname
              union all
              select fuid,
                     cast('金条' as string) as coin_type,
                     case
                       when fact_id in ('1', '15', '101') then
                        '付费'
                       when fact_id in ('138') then
                        '兑换'
                       else
                        null
                     end as coin_source,
                     case
                       when fact_id in ('1', '15', '101') then
                        '付费'
                       when fact_id in ('138') then
                        '话费券'
                       else
                        null
                     end as coin_source_subclass,
                     sum(abs(fact_num)) as coin_num,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from stage_dfqp.pb_currencies_stream_stg
               where fcurrencies_type = '11'
                 and fact_type = 1
                 and fact_id in ('105',
                                 '106',
                                 '122',
                                 '128',
                                 '101',
                                 '139',
                                 '2',
                                 '114',
                                 '120',
                                 '102',
                                 '131',
                                 '138')
                 and dt = '%(statdate)s'
               group by fuid,
                        case
                          when fact_id in ('1', '15', '101') then
                           '付费'
                          when fact_id in ('138') then
                           '兑换'
                          else
                           null
                        end,
                        case
                          when fact_id in ('1', '15', '101') then
                           '付费'
                          when fact_id in ('138') then
                           '话费券'
                          else
                           null
                        end,
                        fbpid,
                        fgamefsk,
                        fgamename,
                        fplatformfsk,
                        fplatformname,
                        fhallfsk,
                        fhallname,
                        fterminaltypefsk,
                        fterminaltypename,
                        fversionfsk,
                        fversionname
              union all
              select fuid,
                     case
                       when fcoins_type = 1 then
                        '银币'
                       when fcoins_type = 11 then
                        '金条'
                       else
                        null
                     end as coin_type,
                     cast('赢牌获得' as string) as coin_source,
                     case
                       when t2.subgame_id is null then
                        '普通牌局'
                       else
                        '金流牌局'
                     end as coin_source_subclass,
                     sum(fgamecoins - cast(fcharge as bigint)) as coin_num,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from stage_dfqp.user_gameparty_stg t1
                left join dw_dfqp.conf_subgame_properties t2
                  on t1.fgame_id = t2.subgame_id
               where fcoins_type in (1, 11)
                 and coalesce(fmatch_id, '0') = '0'
                 and coalesce(fmatch_cfg_id, fmatch_log_id, '0') = '0'
                 and dt = '%(statdate)s'
               group by fuid,
                        case
                          when fcoins_type = 1 then
                           '银币'
                          when fcoins_type = 11 then
                           '金条'
                          else
                           null
                        end,
                        case
                          when t2.subgame_id is null then
                           '普通牌局'
                          else
                           '金流牌局'
                        end,
                        fbpid,
                        fgamefsk,
                        fgamename,
                        fplatformfsk,
                        fplatformname,
                        fhallfsk,
                        fhallname,
                        fterminaltypefsk,
                        fterminaltypename,
                        fversionfsk,
                        fversionname
              union all
              select fuid,
                     case
                       when fitem_id = '0' then
                        '银币'
                       when fitem_id = '1' then
                        '金条'
                       else
                        null
                     end as coin_type,
                     cast('赢牌获得' as string) as coin_source,
                     cast('比赛获奖' as string) as coin_source_subclass,
                     sum(fitem_num) as coin_num,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from stage_dfqp.match_economy_stg
               where fitem_id in ('0', '1')
                 and fio_type = 1
                 and dt = '%(statdate)s'
               group by fuid,
                        case
                          when fitem_id = '0' then
                           '银币'
                          when fitem_id = '1' then
                           '金条'
                          else
                           null
                        end,
                        fbpid,
                        fgamefsk,
                        fgamename,
                        fplatformfsk,
                        fplatformname,
                        fhallfsk,
                        fhallname,
                        fterminaltypefsk,
                        fterminaltypename,
                        fversionfsk,
                        fversionname),
            post_data as
             (select fuid,
                     coin_type,
                     coin_source,
                     coin_source_subclass,
                     coin_num,
                     sum(if(coin_num > 0, coin_num, 0)) over(partition by fbpid, fuid, coin_type, coin_source) as coin_subtotal,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from pre_data)
            insert overwrite table stage_dfqp.user_coin_source_subclass_daily partition (dt = '%(statdate)s')
            select fuid,
                   coin_type,
                   coin_source,
                   coin_source_subclass,
                   coin_num,
                   round(if(coin_num > 0, coin_num, null) /
                         if(coin_subtotal > 0, coin_subtotal, null) * 100) as coin_percent,
                   fbpid,
                   fgamefsk,
                   fgamename,
                   fplatformfsk,
                   fplatformname,
                   fhallfsk,
                   fhallname,
                   fterminaltypefsk,
                   fterminaltypename,
                   fversionfsk,
                   fversionname
              from post_data
             limit 100000000
        """
        res = self.sql_exe(HQL_coin_source_subclass)
        if res != 0:
            return res

        HQL_coin_source = """
            with pre_data as
             (select fuid,
                     coin_type,
                     coin_source,
                     sum(coin_num) as coin_num,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from stage_dfqp.user_coin_source_subclass_daily
               where dt = '%(statdate)s'
               group by fuid,
                        coin_type,
                        coin_source,
                        fbpid,
                        fgamefsk,
                        fgamename,
                        fplatformfsk,
                        fplatformname,
                        fhallfsk,
                        fhallname,
                        fterminaltypefsk,
                        fterminaltypename,
                        fversionfsk,
                        fversionname),
            post_data as
             (select fuid,
                     coin_type,
                     coin_source,
                     coin_num,
                     sum(coin_num) over(partition by fbpid, fuid, coin_type) as coin_subtotal,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from pre_data)
            insert overwrite table stage_dfqp.user_coin_source_daily partition (dt = '%(statdate)s')
            select fuid,
                   coin_type,
                   coin_source,
                   coin_num,
                   round(if(coin_num > coin_subtotal,
                            100,
                            if(coin_num > 0, coin_num, null) /
                            if(coin_subtotal > 0, coin_subtotal, null) * 100)) as coin_percent,
                   fbpid,
                   fgamefsk,
                   fgamename,
                   fplatformfsk,
                   fplatformname,
                   fhallfsk,
                   fhallname,
                   fterminaltypefsk,
                   fterminaltypename,
                   fversionfsk,
                   fversionname
              from post_data
             limit 100000000
        """
        res = self.sql_exe(HQL_coin_source)
        if res != 0:
            return res

        HQL_coin_type = """
            with pre_data as
             (select fuid,
                     coin_type,
                     sum(coin_num) as coin_num,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from stage_dfqp.user_coin_source_daily
               where coin_num > 0
                 and dt = '%(statdate)s'
               group by fuid,
                        coin_type,
                        fbpid,
                        fgamefsk,
                        fgamename,
                        fplatformfsk,
                        fplatformname,
                        fhallfsk,
                        fhallname,
                        fterminaltypefsk,
                        fterminaltypename,
                        fversionfsk,
                        fversionname),
            post_data as
             (select fuid,
                     coin_type,
                     coin_num,
                     sum(if(coin_type = '金条', coin_num * 100, coin_num)) over(partition by fbpid, fuid) as total_coin,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from pre_data)
            insert overwrite table stage_dfqp.user_coin_type_daily partition (dt = '%(statdate)s')
            select fuid,
                   total_coin,
                   coin_type,
                   coin_num,
                   round(if(coin_num > 0,
                            if(coin_type = '金条', coin_num * 100, coin_num),
                            null) / if(total_coin > 0, total_coin, null) * 100) as coin_percent,
                   fbpid,
                   fgamefsk,
                   fgamename,
                   fplatformfsk,
                   fplatformname,
                   fhallfsk,
                   fhallname,
                   fterminaltypefsk,
                   fterminaltypename,
                   fversionfsk,
                   fversionname
              from post_data
             limit 100000000
        """
        res = self.sql_exe(HQL_coin_type)
        if res != 0:
            return res

        HQL_coin_scene_lv1 = """
            with add_together_info as
             (select fuid,
                     case
                       when fcoins_type = 1 then
                        '银币'
                       when fcoins_type = 11 then
                        '金条'
                       else
                        null
                     end as coin_type,
                     cast('赢牌获得' as string) as coin_source,
                     case
                       when t2.subgame_id is null then
                        '普通牌局'
                       else
                        '金流牌局'
                     end as coin_source_subclass,
                     fpname as coin_scene_lv1,
                     sum(fgamecoins - cast(fcharge as bigint)) as coin_num,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from stage_dfqp.user_gameparty_stg t1
                left join dw_dfqp.conf_subgame_properties t2
                  on t1.fgame_id = t2.subgame_id
               where fcoins_type in (1, 11)
                 and coalesce(fmatch_id, '0') = '0'
                 and coalesce(fmatch_cfg_id, fmatch_log_id, '0') = '0'
                 and dt = '%(statdate)s'
               group by fuid,
                        case
                          when fcoins_type = 1 then
                           '银币'
                          when fcoins_type = 11 then
                           '金条'
                          else
                           null
                        end,
                        case
                          when t2.subgame_id is null then
                           '普通牌局'
                          else
                           '金流牌局'
                        end,
                        fpname,
                        fbpid,
                        fgamefsk,
                        fgamename,
                        fplatformfsk,
                        fplatformname,
                        fhallfsk,
                        fhallname,
                        fterminaltypefsk,
                        fterminaltypename,
                        fversionfsk,
                        fversionname
              union all
              select fuid,
                     case
                       when fitem_id = '0' then
                        '银币'
                       when fitem_id = '1' then
                        '金条'
                       else
                        null
                     end as coin_type,
                     cast('赢牌获得' as string) as coin_source,
                     cast('比赛获奖' as string) as coin_source_subclass,
                     fsubname as coin_scene_lv1,
                     sum(fitem_num) as coin_num,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from stage_dfqp.match_economy_stg
               where fitem_id in ('0', '1')
                 and fio_type = 1
                 and dt = '%(statdate)s'
               group by fuid,
                        case
                          when fitem_id = '0' then
                           '银币'
                          when fitem_id = '1' then
                           '金条'
                          else
                           null
                        end,
                        fsubname,
                        fbpid,
                        fgamefsk,
                        fgamename,
                        fplatformfsk,
                        fplatformname,
                        fhallfsk,
                        fhallname,
                        fterminaltypefsk,
                        fterminaltypename,
                        fversionfsk,
                        fversionname),
            middle_data as
             (select fuid,
                     coin_type,
                     coin_source,
                     coin_source_subclass,
                     coin_scene_lv1,
                     coin_num,
                     sum(coin_num) over(partition by fbpid,fuid,coin_type,coin_source,coin_source_subclass) as coin_subtotal,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from add_together_info)
            insert overwrite table stage_dfqp.user_coin_scene_lv1_daily partition (dt = '%(statdate)s')
            select fuid,
                   coin_type,
                   coin_source,
                   coin_source_subclass,
                   coin_scene_lv1,
                   coin_num,
                   case when coin_num <= 0 then null
                        when coin_num > 0 and coin_num >= coin_subtotal then 100
                        when coin_num > 0 and coin_num < coin_subtotal then round(coin_num / coin_subtotal * 100)
                        else null
                   end as coin_percent,
                   fbpid,
                   fgamefsk,
                   fgamename,
                   fplatformfsk,
                   fplatformname,
                   fhallfsk,
                   fhallname,
                   fterminaltypefsk,
                   fterminaltypename,
                   fversionfsk,
                   fversionname
              from middle_data
        """
        res = self.sql_exe(HQL_coin_scene_lv1)
        if res != 0:
            return res

        HQL_coin_scene_lv2 = """
            with add_together_info as
             (select fuid,
                     case
                       when fcoins_type = 1 then
                        '银币'
                       when fcoins_type = 11 then
                        '金条'
                       else
                        null
                     end as coin_type,
                     cast('赢牌获得' as string) as coin_source,
                     case
                       when t2.subgame_id is null then
                        '普通牌局'
                       else
                        '金流牌局'
                     end as coin_source_subclass,
                     fpname as coin_scene_lv1,
                     fsubname as coin_scene_lv2,
                     sum(fgamecoins - cast(fcharge as bigint)) as coin_num,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from stage_dfqp.user_gameparty_stg t1
                left join dw_dfqp.conf_subgame_properties t2
                  on t1.fgame_id = t2.subgame_id
               where fcoins_type in (1, 11)
                 and coalesce(fmatch_id, '0') = '0'
                 and coalesce(fmatch_cfg_id, fmatch_log_id, '0') = '0'
                 and dt = '%(statdate)s'
               group by fuid,
                        case
                          when fcoins_type = 1 then
                           '银币'
                          when fcoins_type = 11 then
                           '金条'
                          else
                           null
                        end,
                        case
                          when t2.subgame_id is null then
                           '普通牌局'
                          else
                           '金流牌局'
                        end,
                        fpname,
                        fsubname,
                        fbpid,
                        fgamefsk,
                        fgamename,
                        fplatformfsk,
                        fplatformname,
                        fhallfsk,
                        fhallname,
                        fterminaltypefsk,
                        fterminaltypename,
                        fversionfsk,
                        fversionname
              union all
              select fuid,
                     case
                       when fitem_id = '0' then
                        '银币'
                       when fitem_id = '1' then
                        '金条'
                       else
                        null
                     end as coin_type,
                     cast('赢牌获得' as string) as coin_source,
                     cast('比赛获奖' as string) as coin_source_subclass,
                     fsubname as coin_scene_lv1,
                     fgsubname as coin_scene_lv2,
                     sum(fitem_num) as coin_num,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from stage_dfqp.match_economy_stg
               where fitem_id in ('0', '1')
                 and fio_type = 1
                 and dt = '%(statdate)s'
               group by fuid,
                        case
                          when fitem_id = '0' then
                           '银币'
                          when fitem_id = '1' then
                           '金条'
                          else
                           null
                        end,
                        fsubname,
                        fgsubname,
                        fbpid,
                        fgamefsk,
                        fgamename,
                        fplatformfsk,
                        fplatformname,
                        fhallfsk,
                        fhallname,
                        fterminaltypefsk,
                        fterminaltypename,
                        fversionfsk,
                        fversionname),
            middle_data as
             (select fuid,
                     coin_type,
                     coin_source,
                     coin_source_subclass,
                     coin_scene_lv1,
                     coin_scene_lv2,
                     coin_num,
                     sum(coin_num) over(partition by fbpid, fuid, coin_type, coin_source,coin_source_subclass, coin_scene_lv1) as coin_subtotal,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname
                from add_together_info)
            insert overwrite table stage_dfqp.user_coin_scene_lv2_daily partition (dt = '%(statdate)s')
            select fuid,
                   coin_type,
                   coin_source,
                   coin_source_subclass,
                   coin_scene_lv1,
                   coin_scene_lv2,
                   coin_num,
                   case when coin_num <= 0 then null
                        when coin_num > 0 and coin_num >= coin_subtotal then 100
                        when coin_num > 0 and coin_num < coin_subtotal then round(coin_num / coin_subtotal * 100)
                        else null
                   end as coin_percent,
                   fbpid,
                   fgamefsk,
                   fgamename,
                   fplatformfsk,
                   fplatformname,
                   fhallfsk,
                   fhallname,
                   fterminaltypefsk,
                   fterminaltypename,
                   fversionfsk,
                   fversionname
              from middle_data
        """
        res = self.sql_exe(HQL_coin_scene_lv2)
        if res != 0:
            return res

        HQL_coin_source_tag = """
            with t1 as
             (select fuid,
                     total_coin,
                     coin_type,
                     coin_type_num,
                     coin_type_percent,
                     fbpid,
                     fgamefsk,
                     fgamename,
                     fplatformfsk,
                     fplatformname,
                     fhallfsk,
                     fhallname,
                     fterminaltypefsk,
                     fterminaltypename,
                     fversionfsk,
                     fversionname,
                     ranking
                from (select fuid,
                             total_coin,
                             coin_type,
                             coin_num as coin_type_num,
                             coin_percent as coin_type_percent,
                             fbpid,
                             fgamefsk,
                             fgamename,
                             fplatformfsk,
                             fplatformname,
                             fhallfsk,
                             fhallname,
                             fterminaltypefsk,
                             fterminaltypename,
                             fversionfsk,
                             fversionname,
                             row_number() over(partition by fbpid, fuid order by coin_percent desc nulls last, coin_type) as ranking
                        from stage_dfqp.user_coin_type_daily
                       where dt = '%(statdate)s') m
               where ranking = 1),
            t2 as
             (select fuid,
                     coin_source,
                     coin_num as coin_source_num,
                     coin_percent as coin_source_percent,
                     fbpid,
                     row_number() over(partition by fbpid, fuid order by if (coin_type = '金条', coin_num * 100, coin_num) desc) as ranking
                from stage_dfqp.user_coin_source_daily
               where dt = '%(statdate)s'),
            t3 as
             (select fuid,
                     coin_source_subclass,
                     coin_num as coin_source_subclass_num,
                     coin_percent as coin_source_subclass_percent,
                     fbpid,
                     row_number() over(partition by fbpid, fuid order by if (coin_type = '金条', coin_num * 100, coin_num) desc) as ranking
                from stage_dfqp.user_coin_source_subclass_daily
               where dt = '%(statdate)s'),
            t4 as
             (select fuid,
                     coin_scene_lv1,
                     coin_num as coin_scene_lv1_num,
                     coin_percent as coin_scene_lv1_percent,
                     fbpid,
                     row_number() over(partition by fbpid, fuid order by if (coin_type = '金条', coin_num * 100, coin_num) desc) as ranking
                from stage_dfqp.user_coin_scene_lv1_daily
               where dt = '%(statdate)s'),
            t5 as
             (select fuid,
                     coin_scene_lv2,
                     coin_num as coin_scene_lv2_num,
                     coin_percent as coin_scene_lv2_percent,
                     fbpid,
                     row_number() over(partition by fbpid, fuid order by if (coin_type = '金条', coin_num * 100, coin_num) desc) as ranking
                from stage_dfqp.user_coin_scene_lv2_daily
               where dt = '%(statdate)s'),
            t6 as
             (select fbpid, fuid, sum(fcoins_num) as pay_sum
                from stage_dfqp.payment_stream_stg
               where dt = '%(statdate)s'
               group by fbpid, fuid)
            insert overwrite table stage_dfqp.user_coin_source_tag partition (dt = '%(statdate)s')
            select t1.fuid,
                   coalesce(t6.pay_sum, 0) as pay_sum,
                   t1.total_coin,
                   t1.coin_type,
                   t1.coin_type_num,
                   if(t1.coin_type_num > 0, t1.coin_type_percent, null) as coin_type_percent,
                   t2.coin_source,
                   t2.coin_source_num,
                   if(t2.coin_source_num > 0, t2.coin_source_percent, null) as coin_source_percent,
                   t3.coin_source_subclass,
                   t3.coin_source_subclass_num,
                   if(t3.coin_source_subclass_num > 0,
                      t3.coin_source_subclass_percent,
                      null) as coin_source_subclass_percent,
                   t4.coin_scene_lv1,
                   t4.coin_scene_lv1_num,
                   if(t4.coin_scene_lv1_num > 0, t4.coin_scene_lv1_percent, null) as coin_scene_lv1_percent,
                   t5.coin_scene_lv2,
                   t5.coin_scene_lv2_num,
                   if(t5.coin_scene_lv2_num > 0, t5.coin_scene_lv2_percent, null) as coin_scene_lv2_percent,
                   t1.fbpid,
                   t1.fgamefsk,
                   t1.fgamename,
                   t1.fplatformfsk,
                   t1.fplatformname,
                   t1.fhallfsk,
                   t1.fhallname,
                   t1.fterminaltypefsk,
                   t1.fterminaltypename,
                   t1.fversionfsk,
                   t1.fversionname
              from t1
              left join t2
                on t1.fbpid = t2.fbpid
               and t1.fuid = t2.fuid
               and t1.ranking = t2.ranking
              left join t3
                on t1.fbpid = t3.fbpid
               and t1.fuid = t3.fuid
               and t1.ranking = t3.ranking
              left join t4
                on t1.fbpid = t4.fbpid
               and t1.fuid = t4.fuid
               and t1.ranking = t4.ranking
              left join t5
                on t1.fbpid = t5.fbpid
               and t1.fuid = t5.fuid
               and t1.ranking = t5.ranking
              left join t6
                on t1.fbpid = t6.fbpid
               and t1.fuid = t6.fuid
        """
        res = self.sql_exe(HQL_coin_source_tag)
        if res != 0:
            return res

        return res


# 实例化执行
a = UserCoinSource(sys.argv[1:])
a()