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


class load_dfqp_pb_gamecoins_stream_stg(BaseStatModel):

    # 将地方棋牌的游戏币流水单独剥离出来

    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.pb_gamecoins_stream_stg
        (
            fbpid               string           comment 'BPID',
            fuid                bigint           comment '用户UID',
            flts_at             string           comment '流水时间戳',
            fact_id             string           comment '操作编号',
            fact_type           tinyint          comment '操作类型',
            fact_num            bigint           comment '数量',
            fuser_gamecoins_num bigint           comment '用户操作后游戏币数量',
            fversion_info       string           comment '版本号',
            fchannel_code       string           comment '渠道邀请ID',
            fvip_type           bigint           comment 'vip类型',
            fvip_level          bigint           comment 'vip等级',
            flevel              bigint           comment '用户等级',
            fseq_no             decimal(38,0)    comment '游戏币变化流水序号',
            fgame_id            int              comment '子游戏ID',
            fpartner_info       varchar(32)      comment '代理商',
            fscene              varchar(100)     comment '场景',
            fgamefsk            bigint           comment '游戏id',
            fgamename           string           comment '游戏名称',
            fplatformfsk        bigint           comment '平台id',
            fplatformname       string           comment '平台名称',
            fhallfsk            bigint           comment '大厅id',
            fhallname           string           comment '大厅名称',
            fterminaltypefsk    bigint           comment '终端类型id',
            fterminaltypename   string           comment '终端类型名称',
            fversionfsk         bigint           comment '版本id',
            fversionname        string           comment '版本名称', 
            fbank_gamecoins		bigint			 comment '保险箱游戏币额度'
        )comment '地方棋牌游戏币流水'
        partitioned by(dt string)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res


    def stat(self):

        hql = """
            insert overwrite table stage_dfqp.pb_gamecoins_stream_stg
            partition(dt='%(statdate)s')
            select distinct a.fbpid,
                    case when b.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid,
                    lts_at flts_at,
                    act_id fact_id,
                    act_type fact_type,
                    act_num fact_num,
                    user_gamecoins_num fuser_gamecoins_num,
                    fversion_info,
                    fchannel_code,
                    fvip_type,
                    fvip_level,
                    flevel,
                    fseq_no,
                    fgame_id,
                    fpartner_info,
                    fscene,
                    b.fgamefsk,
                    b.fgamename,
                    b.fplatformfsk,
                    b.fplatformname,
                    b.fhallfsk,
                    b.fhallname,
                    b.fterminaltypefsk,
                    b.fterminaltypename,
                    b.fversionfsk,
                    b.fversionname, 
                    fbank_gamecoins 
                from stage.pb_gamecoins_stream_stg a
                join dim.bpid_map b
                  on a.fbpid=b.fbpid
                 and b.fgamefsk = 4132314431
                where a.dt = '%(statdate)s'
        distribute by a.fbpid, fuid
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res

# 生成统计实例
a = load_dfqp_pb_gamecoins_stream_stg(sys.argv[1:])
a()
