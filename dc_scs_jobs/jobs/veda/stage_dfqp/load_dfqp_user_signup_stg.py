#-*- coding: UTF-8 -*-
import os
import sys
import time
import datetime

from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))

from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class load_dfqp_user_signup_stg(BaseStatModel):

    # 将地方棋牌的注册流水单独剥离出来
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.user_signup_stg
        ( fbpid                     varchar(50)      comment 'BPID',
          fuid                      bigint           comment '用户ID',
          fplatform_uid             varchar(50)      comment '平台UID',
          fsignup_at                string           comment '注册时间',
          fip                       varchar(20)      comment '注册IP',
          fgender                   tinyint          comment '用户性别',
          fage                      tinyint          comment '用户年龄',
          flanguage                 varchar(50)      comment '语言/语种',
          fcountry                  varchar(50)      comment '国家',
          fcity                     varchar(50)      comment '城市',
          ffriends_num              bigint           comment '平台好友数量',
          fappfriends_num           bigint           comment '加入应用好友数量',
          fprofession               varchar(50)      comment '用户职业',
          fentrance_id              bigint           comment '用户登入入口',
          fversion_info             varchar(50)      comment '游戏版本号',
          fchannel_code             int              comment '渠道ID',
          fad_code                  varchar(50)      comment '广告激活ID',
          fm_dtype                  varchar(100)     comment '终端设备机型',
          fm_pixel                  varchar(100)     comment '终端设备屏幕尺寸大小',
          fm_imei                   varchar(100)     comment '终端设备号',
          fm_os                     varchar(100)     comment '终端设备操作系统',
          fm_network                varchar(100)     comment '终端联网接入方式',
          fm_operator               varchar(100)     comment '终端设备网络运营商',
          fmnick                    varchar(64)      comment '用户昵称',
          fmname                    varchar(64)      comment '用户真实姓名',
          femail                    varchar(64)      comment '用户邮箱',
          fmobilesms                varchar(64)      comment '用户手机号',
          fsource_path              varchar(100)     comment '来源路径',
          fip_country               string           comment 'IP对应国家',
          fip_province              string           comment 'IP对应省份',
          fip_city                  string           comment 'IP对应城市',
          fip_countrycode           string           comment 'IP对应国家代码',
          flatitude                 string           comment '用户所在位置的纬度',
          flongitude                string           comment '用户所在位置的经度',
          fpartner_info             varchar(32)      comment '代理商UID',
          fpromoter                 varchar(100)     comment '推广员',
          fshare_key                varchar(100)     comment '新用户注册是来自于某个分享的key',
          fm_imsi                   string           comment '移动用户的IMSI（国际移动用户识别码）',
          fcid                      string           comment '用户cid',
          fsimulator_flag           int              comment '模拟器标识（1、3、4为真机，2、5为模拟器）',
          fcpu_type                 string           comment '用户设备CPU型号',
          fgamefsk                  bigint           comment '游戏id',
          fgamename                 string           comment '游戏名称',
          fplatformfsk              bigint           comment '平台id',
          fplatformname             string           comment '平台名称',
          fhallfsk                  bigint           comment '大厅id',
          fhallname                 string           comment '大厅名称',
          fterminaltypefsk          bigint           comment '终端类型id',
          fterminaltypename         string           comment '终端类型名称',
          fversionfsk               bigint           comment '版本id',
          fversionname              string           comment '版本名称'
        )comment '地方棋牌注册流水'
        partitioned by(dt string)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.user_signup_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid
               ,case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid
               ,t1.fplatform_uid
               ,t1.fsignup_at
               ,t1.fip
               ,t1.fgender
               ,t1.fage
               ,t1.flanguage
               ,t1.fcountry
               ,t1.fcity
               ,t1.ffriends_num
               ,t1.fappfriends_num
               ,t1.fprofession
               ,t1.fentrance_id
               ,t1.fversion_info
               ,t1.fchannel_code
               ,t1.fad_code
               ,t1.fm_dtype
               ,t1.fm_pixel
               ,t1.fm_imei
               ,t1.fm_os
               ,t1.fm_network
               ,t1.fm_operator
               ,t1.fmnick
               ,t1.fmname
               ,t1.femail
               ,t1.fmobilesms
               ,t1.fsource_path
               ,t1.fip_country
               ,t1.fip_province
               ,t1.fip_city
               ,t1.fip_countrycode
               ,t1.flatitude
               ,t1.flongitude
               ,t1.fpartner_info
               ,t1.fpromoter
               ,t1.fshare_key
               ,t1.fm_imsi
               ,t1.fcid
               ,t1.fsimulator_flag
               ,t1.fcpu_type
               ,tt.fgamefsk
               ,tt.fgamename
               ,tt.fplatformfsk
               ,tt.fplatformname
               ,tt.fhallfsk
               ,tt.fhallname
               ,tt.fterminaltypefsk
               ,tt.fterminaltypename
               ,tt.fversionfsk
               ,tt.fversionname
        from stage.user_signup_stg t1
        join dim.bpid_map_bud tt
          on t1.fbpid = tt.fbpid
         and fgamefsk = 4132314431
        where dt = '%(statdate)s'
        distribute by t1.fbpid, fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = load_dfqp_user_signup_stg(sys.argv[1:])
a()
