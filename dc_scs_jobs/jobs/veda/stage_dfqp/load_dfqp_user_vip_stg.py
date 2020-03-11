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


class load_dfqp_user_vip_stg(BaseStatModel):

    # 将地方棋牌的vip流水单独剥离出来
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.user_vip_stg
        ( fbpid             varchar(50)       comment 'BPID',
          fuid              bigint            comment '用户UID',
          fvip_at           string            comment '支付vip服务的流水时间',
          fdue_at           string            comment '过期时间（vip服务什么时候到期）',
          fvip_type         int               comment 'VIP类别',
          fplatform_uid     varchar(100)      comment '用户平台id',
          fvip_level        int               comment 'vip当前等级（续费时有效）',
          flevel            int               comment '游戏等级',
          foper_type        varchar(100)      comment '操作类型',
          foper_way         varchar(100)      comment '操作方式',
          fpay_uid          bigint            comment '支付用户的uid',
          fpay_way          varchar(100)      comment '购买方式/获得方式',
          fpay_info         varchar(100)      comment '支付信息（记录一些支付过程中的信息）',
          fip               varchar(100)      comment '客户端ip',
          fmoney            decimal(20,2)     comment '用户购买vip花费的货币',
          fdays             bigint            comment '本次购买的天数（天是最小单位）',
          ffirst_at         string            comment '首次开通vip的时间',
          flast_due_at      string            comment '上次vip结束时间',
          fversion_info     varchar(100)      comment '版本号',
          fchannel_code     varchar(100)      comment '渠道邀请ID',
          fgamefsk          bigint            comment '游戏id',
          fgamename         string            comment '游戏名称',
          fplatformfsk      bigint            comment '平台id',
          fplatformname     string            comment '平台名称',
          fhallfsk          bigint            comment '大厅id',
          fhallname         string            comment '大厅名称',
          fterminaltypefsk  bigint            comment '终端类型id',
          fterminaltypename string            comment '终端类型名称',
          fversionfsk       bigint            comment '版本id',
          fversionname      string            comment '版本名称'
        )comment '地方棋牌vip流水'
        partitioned by(dt string)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.user_vip_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid
               ,case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid
               ,fvip_at
               ,fdue_at
               ,fvip_type
               ,fplatform_uid
               ,fvip_level
               ,flevel
               ,foper_type
               ,foper_way
               ,fpay_uid
               ,fpay_way
               ,fpay_info
               ,fip
               ,fmoney
               ,fdays
               ,ffirst_at
               ,flast_due_at
               ,fversion_info
               ,fchannel_code
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
        from stage.user_vip_stg t1
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
a = load_dfqp_user_vip_stg(sys.argv[1:])
a()
