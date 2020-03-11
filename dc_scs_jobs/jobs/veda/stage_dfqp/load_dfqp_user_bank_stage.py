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


class load_dfqp_user_bank_stage(BaseStatModel):

    # 将地方棋牌的保险箱流水单独剥离出来
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.user_bank_stage
        ( fbpid                    varchar(50)       comment 'BPID',
          fuid                     bigint            comment '用户UID',
          flts_at                  string            comment '日志上报时间戳',
          fip                      varchar(128)      comment 'ip地址',
          fact_type                int               comment '存取的操作码',
          fact_num                 bigint            comment '操作额度',
          fbank_gamecoins_num      bigint            comment '存取后保险箱额度值',
          fuser_gamecoins_num      bigint            comment '存取后用户身上携带额度',
          fdesc                    varchar(20)       comment '描述',
          fversion_info            varchar(100)      comment '版本号',
          fchannel_code            varchar(100)      comment '渠道邀请ID',
          fgame_id                 int               comment '子游戏ID',
          fcurrencies_type         varchar(100)      comment '货币类型',
          fgamefsk                 bigint            comment '游戏id',
          fgamename                string            comment '游戏名称',
          fplatformfsk             bigint            comment '平台id',
          fplatformname            string            comment '平台名称',
          fhallfsk                 bigint            comment '大厅id',
          fhallname                string            comment '大厅名称',
          fterminaltypefsk         bigint            comment '终端类型id',
          fterminaltypename        string            comment '终端类型名称',
          fversionfsk              bigint            comment '版本id',
          fversionname             string            comment '版本名称', 
          fseq_no				   bigint			 comment '货币变化流水序号'
        )comment '地方棋牌保险箱流水'
        partitioned by(dt string)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.user_bank_stage
        partition(dt='%(statdate)s')
        select distinct t1.fbpid
               ,case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid
               ,flts_at
               ,fip
               ,fact_type
               ,fact_num
               ,fbank_gamecoins_num
               ,fuser_gamecoins_num
               ,fdesc
               ,fversion_info
               ,fchannel_code
               ,fgame_id
               ,fcurrencies_type
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
               ,fseq_no 
        from stage.user_bank_stage t1
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
a = load_dfqp_user_bank_stage(sys.argv[1:])
a()
