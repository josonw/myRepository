#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserAbnormalRobSuspect(BaseStatModel):
    """疑似盗号玩家信息统计"""
    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS veda.user_abnormal_rob_suspect
            (
              suspect_mid bigint COMMENT '嫌疑mid',
              hallname string COMMENT '地区',
              victim_mid bigint COMMENT '被盗mid',
              seize_silver_coin bigint COMMENT '攫取被盗mid银币数（非精确值，仅供参考）',
              seize_gold_bar bigint COMMENT '攫取被盗mid金条数（非精确值，仅供参考）',
              if_bull string COMMENT '是否有凑但没凑',
              remain_silver_coin bigint COMMENT '被盗mid剩余银币',
              remain_gold_bar bigint COMMENT '被盗mid剩余金条',
              device_imei string COMMENT '设备标识',
              device_type string COMMENT '设备型号',
              device_pixel string COMMENT '分辨率',
              if_same_device string COMMENT '是否和被盗玩家设备一致',
              app_version string COMMENT '版本号',
              device_os string COMMENT '系统类型',
              ip string COMMENT 'IP',
              ip_country string COMMENT 'IP所在国家',
              ip_province string COMMENT 'IP所在省份',
              ip_city string COMMENT 'IP所在城市',
              if_same_ip string COMMENT '是否和被盗玩家IP一致'
            ) COMMENT '疑似盗号玩家'
            partitioned by (dt string)
            stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            --被盗用户名单
            with victim_users as
             (select distinct fuid
                from veda.user_abnormal_login_info
               where dt = '%(statdate)s'
               order by fuid),
            --涉及牌局
            involved_gameparty as
             (select distinct v1.fuid, t2.fplatformfsk, t2.fgamefsk, t2.finning_id
                from victim_users v1
               inner join stage_dfqp.user_gameparty_stg t2
                  on (v1.fuid = t2.fuid and t2.dt = '%(statdate)s')
               order by t2.fplatformfsk, t2.fgamefsk, t2.finning_id),
            --嫌疑人名单
            suspect_users as
             (select t2.fuid as suspect_mid,
                     v2.fuid as victim_mid,
                     sum(case
                           when t2.fcoins_type = 1 then
                            t2.fgamecoins
                           else
                            0
                         end) as seize_silver_coin,
                     sum(case
                           when t2.fcoins_type = 11 then
                            t2.fgamecoins
                           else
                            0
                         end) as seize_gold_bar
                from involved_gameparty v2
               inner join stage_dfqp.user_gameparty_stg t2
                  on (v2.fplatformfsk = t2.fplatformfsk and v2.fgamefsk = t2.fgamefsk and
                     v2.finning_id = t2.finning_id and t2.dt = '%(statdate)s')
               where v2.fuid != t2.fuid
               group by t2.fuid, v2.fuid
               order by suspect_mid, victim_mid),
            --被盗用户详细信息
            victim_info as
             (select v1.fuid,
                     t3.total_silver_coin,
                     t3.total_gold_bar,
                     t3.latest_device_imei,
                     t3.latest_login_ip
                from victim_users v1
               inner join veda.dfqp_user_portrait t3
                  on v1.fuid = t3.mid
               order by v1.fuid),
            --嫌疑人详细信息
            suspect_info as
             (select v3.suspect_mid,
                     t3.fhallname,
                     v3.victim_mid,
                     v3.seize_silver_coin,
                     v3.seize_gold_bar,
                     t3.latest_device_imei,
                     t3.latest_device_type,
                     t3.latest_device_pixel,
                     t3.latest_app_version,
                     t3.latest_device_os,
                     t3.latest_login_ip,
                     t3.latest_login_ip_country,
                     t3.latest_login_ip_province,
                     t3.latest_login_ip_city
                from suspect_users v3
               inner join veda.dfqp_user_portrait t3
                  on v3.suspect_mid = t3.mid
               where (v3.seize_silver_coin > 0 or v3.seize_gold_bar > 0)
               order by v3.victim_mid)
            insert overwrite table veda.user_abnormal_rob_suspect partition (dt = '%(statdate)s')
            select v5.suspect_mid,
                   v5.fhallname,
                   v5.victim_mid,
                   v5.seize_silver_coin,
                   v5.seize_gold_bar,
                   cast(null as string) as if_bull,
                   v4.total_silver_coin,
                   v4.total_gold_bar,
                   v5.latest_device_imei,
                   v5.latest_device_type,
                   v5.latest_device_pixel,
                   case
                     when v4.latest_device_imei = v5.latest_device_imei then
                      'Y'
                     else
                      'N'
                   end as if_same_device,
                   v5.latest_app_version,
                   v5.latest_device_os,
                   v5.latest_login_ip,
                   v5.latest_login_ip_country,
                   v5.latest_login_ip_province,
                   v5.latest_login_ip_city,
                   case
                     when v4.latest_login_ip = v5.latest_login_ip then
                      'Y'
                     else
                      'N'
                   end as if_same_ip
              from victim_info v4
             inner join suspect_info v5
                on v4.fuid = v5.victim_mid
             order by v5.victim_mid, v5.suspect_mid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化，运行
a = UserAbnormalRobSuspect(sys.argv[1:])
a()