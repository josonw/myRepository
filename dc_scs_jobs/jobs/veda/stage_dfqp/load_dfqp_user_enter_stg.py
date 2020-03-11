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


class load_dfqp_user_enter_stg(BaseStatModel):

    # 将地方棋牌的用户进入子游戏流水单独剥离出来
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.user_enter_stg
        ( 	fbpid				varchar(50) 	comment 'BPID',
			fuid                bigint          comment '用户UID',
			fplatform_uid       varchar(50)     comment '游戏平台uid',
			flts_at             string          comment '进入时间',
			fis_first           tinyint         comment '是否历史首次进入该子游戏',
			fgame_id            int             comment '子游戏ID',
			fchannel_code       varchar(100)    comment '渠道邀请ID',
			fip                 varchar(64)     comment '用户IP地址',
			fentrance_id        bigint          comment '移动登入入口',
			fversion_info       varchar(50)     comment '版本号',
			fad_code            varchar(50)     comment '广告激活ID',
			user_gamecoins      bigint          comment '用户进入该子游戏时携带的游戏币数额',
			flang               varchar(64)     comment '用户进入该子游戏时使用的语言',
			fm_dtype            varchar(100)    comment '终端设备型号/手机机型',
			fm_pixel            varchar(100)    comment '手机机屏大小',
			fm_imei             varchar(100)    comment '手机设备号',
			fm_os               varchar(100)    comment '手机设备/终端操作系统',
			fm_network          varchar(100)    comment '手机设备接入方式',
			fm_operator         varchar(100)    comment '网络运营商',
			fsource_path        varchar(100)    comment '来源路径',
			fmobilesms          string          comment '用户手机号',
			fpname              string          comment '比赛场名称',
			fsubname            string          comment '二级赛名称',
			fgsubname           string          comment '三级赛名称',
			fgamefsk			bigint			comment '游戏ID',
			fgamename			string			comment '游戏名称',
			fplatformfsk		bigint			comment '平台ID',
			fplatformname		string			comment '平台名称',
			fhallfsk			bigint			comment '大厅ID',
			fhallname			string			comment '大厅名称',
			fterminaltypefsk	bigint			comment '终端ID',
			fterminaltypename	string			comment '终端名称',
			fversionfsk			bigint			comment '版本ID',
			fversionname		string			comment '版本名称'

        ) comment '地方棋牌用户进入子游戏流水'
        partitioned by(dt string comment '日期')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.user_enter_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid,
				case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid,
				fplatform_uid,
				flts_at,
				fis_first,
				fgame_id,
				fchannel_code,
				fip,
				fentrance_id,
				fversion_info,
				fad_code,
				user_gamecoins,
				flang,
				fm_dtype,
				fm_pixel,
				fm_imei,
				fm_os,
				fm_network,
				fm_operator,
				fsource_path,
				fmobilesms,
				fpname,
				fsubname,
				fgsubname,
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
        from stage.user_enter_stg t1
        join dim.bpid_map tt
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
a = load_dfqp_user_enter_stg(sys.argv[1:])
a()
