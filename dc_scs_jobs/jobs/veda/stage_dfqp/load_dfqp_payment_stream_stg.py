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


class load_dfqp_payment_stream_stg(BaseStatModel):

    # 将地方棋牌的用户成功订单状态数据流水单独剥离出来
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.payment_stream_stg
        ( 	fbpid					varchar(50)  	comment 'BPID',
			fdate                   string          comment '订单时间',
			fplatform_uid           varchar(50)     comment '平台uid',
			fis_platform_uid        tinyint         comment '是否是平台uid',
			forder_id               varchar(255)    comment '订单id',
			fcoins_num              decimal(20,2)   comment '原币额度',
			frate                   decimal(20,7)   comment '美元兑原币汇率',
			fm_id                   varchar(256)    comment '支付渠道id',
			fm_name                 varchar(256)    comment '支付渠道名称',
			fp_id                   varchar(256)    comment '产品id',
			fp_name                 varchar(256)    comment '产品名称',
			fchannel_id             varchar(64)     comment '用户渠道id',
			fimei                   varchar(64)     comment '设备号',
			fsucc_time              string          comment '成功时间',
			fcallback_time          string          comment '回调时间',
			fp_type                 int             comment '商品类型',
			fp_num                  bigint          comment '商品数量',
			fsid                    int             comment '平台ID',
			fappid                  int             comment '应用ID',
			fpmode                  int             comment '渠道ID',
			fuid                    bigint          comment '用户UID',
			fpamount_usd            decimal(20,2)   comment '美金额度',
			fproduct_id             varchar(64)     comment '业务上报的商品ID',
			fproduct_name           varchar(64)     comment '业务上报的商品名称',
			fip                     varchar(64)     comment '订单IP',
			fcid                    string          comment '支付时的用户cid',
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
        ) comment '地方棋牌用户成功订单状态数据流水'
        partitioned by(dt string comment '日期')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.payment_stream_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid,
				fdate,
				fplatform_uid,
				fis_platform_uid,
				forder_id,
				fcoins_num,
				frate,
				fm_id,
				fm_name,
				fp_id,
				fp_name,
				fchannel_id,
				fimei,
				fsucc_time,
				fcallback_time,
				fp_type,
				fp_num,
				fsid,
				fappid,
				fpmode,
				case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid,
				fpamount_usd,
				fproduct_id,
				fproduct_name,
				fip,
				fcid,
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
        from stage.payment_stream_stg t1
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
a = load_dfqp_payment_stream_stg(sys.argv[1:])
a()
