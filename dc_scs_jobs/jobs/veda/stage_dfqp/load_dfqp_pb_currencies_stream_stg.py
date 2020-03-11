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


class load_dfqp_pb_currencies_stream_stg(BaseStatModel):

    # 将地方棋牌的货币流水单独剥离出来
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.pb_currencies_stream_stg
        ( fbpid              varchar(50)     comment 'BPID',
          fuid               bigint          comment '用户UID',
          flts_at            string          comment '流水时间戳',
          fcurrencies_type   varchar(32)     comment '货币类型',
          fact_type          int             comment '操作类型',
          fact_id            varchar(32)     comment '操作编号',
          fact_num           bigint          comment '数量',
          fcurrencies_num    bigint          comment '用户当前货币数量（操作后的货币数量）',
          fseq_no            decimal(38,0)   comment '货币变化流水序号',
          fversion_info      varchar(50)     comment '版本号',
          fchannel_code      varchar(100)    comment '渠道邀请ID',
          fgame_id           int             comment '子游戏ID',
          fscene             varchar(100)    comment '场景',
          fgamefsk           bigint          comment '游戏id',
          fgamename          string          comment '游戏名称',
          fplatformfsk       bigint          comment '平台id',
          fplatformname      string          comment '平台名称',
          fhallfsk           bigint          comment '大厅id',
          fhallname          string          comment '大厅名称',
          fterminaltypefsk   bigint          comment '终端类型id',
          fterminaltypename  string          comment '终端类型名称',
          fversionfsk        bigint          comment '版本id',
          fversionname       string          comment '版本名称', 
          fbank_currencies	 bigint			 comment '保险箱额度'
        )comment '地方棋牌货币流水'
        partitioned by(dt string)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.pb_currencies_stream_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid
               ,case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid
               ,flts_at
               ,fcurrencies_type
               ,fact_type
               ,fact_id
               ,fact_num
               ,fcurrencies_num
               ,fseq_no
               ,fversion_info
               ,fchannel_code
               ,fgame_id
               ,fscene
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
               ,fbank_currencies 
        from stage.pb_currencies_stream_stg t1
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
a = load_dfqp_pb_currencies_stream_stg(sys.argv[1:])
a()
