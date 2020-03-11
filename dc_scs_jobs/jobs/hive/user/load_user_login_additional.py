# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class load_user_login_additional(BaseStatModel):
    def create_tab(self):

        hql = """--用户城市切换信息表
        create table if not exists dim.user_login_additional
        (fbpid            varchar(50)        comment   'BPID',
         fuid             bigint             comment   '用户UID',
         fplatform_uid    varchar(50)        comment   '用户平台ID',
         flogin_at        string             comment   '登录时间',
         fip              varchar(20)        comment   '登录IP',
         fis_first        tinyint            comment   '是否为首次登入',
         freference       varchar(4000)      comment   '引用来源链接',
         fhttp_version    varchar(400)       comment   'HTTP版本',
         fuser_agent      varchar(4000)      comment   '用户代理',
         fentrance_id     bigint             comment   '移动登入入口',
         fversion_info    varchar(50)        comment   '版本号',
         fchannel_code    varchar(50)        comment   '渠道ID',
         fad_code         varchar(50)        comment   '广告激活ID',
         ffirst_at        string             comment   '首次登录时间',
         user_gamecoins   bigint             comment   '携带游戏币数额',
         flang            varchar(64)        comment   '语言',
         fm_dtype         varchar(100)       comment   '手机机型/终端设备型号',
         fm_pixel         varchar(100)       comment   '手机机屏大小',
         fm_imei          varchar(100)       comment   '手机设备号',
         fm_os            varchar(100)       comment   '手机操作系统',
         fm_network       varchar(100)       comment   '手机设备接入方式',
         fm_operator      varchar(100)       comment   '运营商',
         fsource_path     varchar(100)       comment   '来源路径',
         fvip_type        varchar(100)       comment   'VIP类型',
         fvip_level       int                comment   'VIP等级',
         flevel           int                comment   '等级',
         fip_country      varchar(128)       comment   '登录IP所属国家',
         fip_province     varchar(128)       comment   '登录IP所属省份',
         fip_city         varchar(128)       comment   '登录IP所属城市',
         fip_countrycode  varchar(32)        comment   '登录IP所属国家ID',
         user_bycoins     bigint             comment   '携带博雅币数额',
         bank_gamecoins   bigint             comment   '保险箱数额',
         fip_latitude     varchar(32)        comment   '登录IP所在位置的经度',
         fip_longitude    varchar(32)        comment   '登录IP所在位置的纬度',
         flatitude        string             comment   '用户所在位置的纬度',
         flongitude       string             comment   '用户所在位置的经度',
         fpartner_info    varchar(32)        comment   '代理商',
         fpromoter        varchar(100)       comment   '推广员',
         fm_imsi          string             comment   '移动用户的IMSI（国际移动用户识别码）',
         fsimulator_flag  string             comment   '模拟器标识',
         fmobilesms       string             comment   '用户手机号',
         flag             int                comment '标识位：1是上报数据，2是处理过的数据'
        )comment '登录用户补充省包切换用户'
               partitioned by(dt string)
        stored as orc;
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


    def stat(self):
        """ 重要部分，统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.user_login_additional partition (dt='%(statdate)s')
        select fbpid
               ,fuid
               ,fplatform_uid
               ,flogin_at
               ,fip
               ,fis_first
               ,freference
               ,fhttp_version
               ,fuser_agent
               ,fentrance_id
               ,fversion_info
               ,fchannel_code
               ,fad_code
               ,ffirst_at
               ,user_gamecoins
               ,flang
               ,fm_dtype
               ,fm_pixel
               ,fm_imei
               ,fm_os
               ,fm_network
               ,fm_operator
               ,fsource_path
               ,fvip_type
               ,fvip_level
               ,flevel
               ,fip_country
               ,fip_province
               ,fip_city
               ,fip_countrycode
               ,user_bycoins
               ,bank_gamecoins
               ,fip_latitude
               ,fip_longitude
               ,flatitude
               ,flongitude
               ,fpartner_info
               ,fpromoter
               ,fm_imsi
               ,fsimulator_flag
               ,fmobilesms
               ,1 flag
          from stage.user_login_stg t1
         where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        hql = """
        insert into table dim.user_login_additional partition (dt='%(statdate)s')
        select t2.fp_bpid fbpid
               ,t2.fuid
               ,fplatform_uid
               ,flogin_at
               ,fip
               ,fis_first
               ,freference
               ,fhttp_version
               ,fuser_agent
               ,fentrance_id
               ,fversion_info
               ,fchannel_code
               ,fad_code
               ,ffirst_at
               ,user_gamecoins
               ,flang
               ,fm_dtype
               ,fm_pixel
               ,fm_imei
               ,fm_os
               ,fm_network
               ,fm_operator
               ,fsource_path
               ,fvip_type
               ,fvip_level
               ,flevel
               ,fip_country
               ,fip_province
               ,fip_city
               ,fip_countrycode
               ,user_bycoins
               ,bank_gamecoins
               ,fip_latitude
               ,fip_longitude
               ,flatitude
               ,flongitude
               ,fpartner_info
               ,fpromoter
               ,fm_imsi
               ,fsimulator_flag
               ,fmobilesms
               ,2 flag
          from dim.user_login_additional t1
          join (select distinct fbpid,fuid,fp_bpid from dim.city_change_day t where dt = '%(statdate)s') t2
            on t1.fbpid = t2.fbpid
           and t1.fuid = t2.fuid
         where t1.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res


# 生成统计实例
a = load_user_login_additional(sys.argv[1:])
a()
