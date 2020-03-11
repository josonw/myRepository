#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserLoginStg(BaseStatModel):

    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS stage_dfqp.user_login_stg(
             fbpid varchar(50) COMMENT 'BPID',
             fuid bigint COMMENT '用户ID',
             fplatform_uid varchar(50) COMMENT '用户平台ID',
             flogin_at string COMMENT '登录时间',
             fip varchar(20) COMMENT '登录IP',
             fis_first tinyint COMMENT '是否首次登入',
             freference varchar(4000) COMMENT '引用来源链接',
             fhttp_version varchar(400) COMMENT 'HTTP版本',
             fuser_agent varchar(4000) COMMENT '用户代理',
             fentrance_id bigint COMMENT '登录入口',
             fversion_info varchar(50) COMMENT '版本号',
             fchannel_code varchar(50) COMMENT '渠道ID',
             fad_code varchar(50) COMMENT '广告激活ID',
             ffirst_at string COMMENT '首次登录时间',
             user_gamecoins bigint COMMENT '携带游戏币数量',
             flang varchar(64) COMMENT '语言',
             fm_dtype varchar(100) COMMENT '设备型号',
             fm_pixel varchar(100) COMMENT '屏幕大小',
             fm_imei varchar(100) COMMENT '设备串号',
             fm_os varchar(100) COMMENT '操作系统',
             fm_network varchar(100) COMMENT '设备接入方式',
             fm_operator varchar(100) COMMENT '运营商',
             fsource_path varchar(100) COMMENT '来源路径',
             fvip_type varchar(100) COMMENT 'VIP类型',
             fvip_level int COMMENT 'VIP等级',
             flevel int COMMENT '游戏等级',
             fip_country varchar(128) COMMENT '登录IP所属国家',
             fip_province varchar(128) COMMENT '登录IP所属省份',
             fip_city varchar(128) COMMENT '登录IP所属城市',
             fip_countrycode varchar(32) COMMENT '登录IP所属国家代码',
             user_bycoins bigint COMMENT '携带博雅币数量',
             bank_gamecoins bigint COMMENT '保险箱游戏币数量',
             fip_latitude varchar(32) COMMENT '登录IP所在地经度',
             fip_longitude varchar(32) COMMENT '登录IP所在地纬度',
             flatitude string COMMENT '用户位置纬度',
             flongitude string COMMENT '用户位置经度',
             fpartner_info varchar(32) COMMENT '代理商',
             fpromoter varchar(100) COMMENT '推广员',
             fm_imsi string COMMENT '用户IMSI(国际移动用户识别码)',
             fsimulator_flag string COMMENT '模拟器标识',
             fmobilesms string COMMENT '用户手机号码',
             fsign_excption_flag int COMMENT '异常新增标识',
             fwave_flag int COMMENT '波动计标识',
             factive_excption_flag int COMMENT '异常活跃标识',
             fmutil_account_flag int COMMENT '多账号标识',
             fcpu_type string COMMENT '设备CPU型号',
             fgamefsk bigint,
             fgamename string,
             fplatformfsk bigint,
             fplatformname string,
             fhallfsk bigint,
             fhallname string,
             fterminaltypefsk bigint,
             fterminaltypename string,
             fversionfsk bigint,
             fversionname string)
            COMMENT '地方棋牌用户登录记录'
            PARTITIONED BY (dt string COMMENT '日期')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            insert overwrite table stage_dfqp.user_login_stg partition (dt='%(statdate)s')
            select distinct t2.fbpid,
                   case
                     when t1.fplatformfsk = 77000221 and t2.fuid < 1000000000 then
                      t2.fuid + 1000000000
                     else
                      t2.fuid
                   end as fuid,
                   t2.fplatform_uid,
                   t2.flogin_at,
                   t2.fip,
                   t2.fis_first,
                   t2.freference,
                   t2.fhttp_version,
                   t2.fuser_agent,
                   t2.fentrance_id,
                   t2.fversion_info,
                   t2.fchannel_code,
                   t2.fad_code,
                   t2.ffirst_at,
                   t2.user_gamecoins,
                   t2.flang,
                   t2.fm_dtype,
                   t2.fm_pixel,
                   t2.fm_imei,
                   t2.fm_os,
                   t2.fm_network,
                   t2.fm_operator,
                   t2.fsource_path,
                   t2.fvip_type,
                   t2.fvip_level,
                   t2.flevel,
                   t2.fip_country,
                   t2.fip_province,
                   t2.fip_city,
                   t2.fip_countrycode,
                   t2.user_bycoins,
                   t2.bank_gamecoins,
                   t2.fip_latitude,
                   t2.fip_longitude,
                   t2.flatitude,
                   t2.flongitude,
                   t2.fpartner_info,
                   t2.fpromoter,
                   t2.fm_imsi,
                   t2.fsimulator_flag,
                   t2.fmobilesms,
                   t2.fsign_excption_flag,
                   t2.fwave_flag,
                   t2.factive_excption_flag,
                   t2.fmutil_account_flag,
                   t2.fcpu_type,
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
              from dim.bpid_map_bud t1
             inner join stage.user_login_stg t2
                on (t1.fbpid = t2.fbpid and t1.fgamefsk = 4132314431)
             where t2.dt = '%(statdate)s'
             distribute by t2.fbpid
              sort by fuid asc nulls last, t2.flogin_at asc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
a = UserLoginStg(sys.argv[1:])
a()