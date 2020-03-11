#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class UserBankruptRelieveStg(BaseStatModel):

    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS stage_dfqp.user_bankrupt_relieve_stg(
             fbpid varchar(50) COMMENT 'BPID',
             fuid bigint COMMENT '用户ID',
             fcnt bigint COMMENT '破产救济领取次数',
             flts_at string COMMENT '领取时间',
             fgamecoins bigint COMMENT '救济额度',
             fuser_grade int COMMENT '用户当前等级',
             fversion_info varchar(50) COMMENT '版本号',
             fchannel_code varchar(100) COMMENT '渠道邀请ID',
             fvip_type varchar(100) COMMENT 'VIP类型',
             fvip_level int COMMENT 'VIP等级',
             flevel int COMMENT '游戏等级',
             fpname varchar(100) COMMENT '牌局一级场次',
             fgame_id int COMMENT '子游戏ID',
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
            COMMENT '地方棋牌破产救济领取记录'
            PARTITIONED BY (dt string COMMENT '日期')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            insert overwrite table stage_dfqp.user_bankrupt_relieve_stg partition (dt='%(statdate)s')
            select distinct t2.fbpid,
                   case
                     when t1.fplatformfsk = 77000221 and t2.fuid < 1000000000 then
                      t2.fuid + 1000000000
                     else
                      t2.fuid
                   end as fuid,
                   t2.fcnt,
                   t2.flts_at,
                   t2.fgamecoins,
                   t2.fuser_grade,
                   t2.fversion_info,
                   t2.fchannel_code,
                   t2.fvip_type,
                   t2.fvip_level,
                   t2.flevel,
                   t2.fpname,
                   t2.fgame_id,
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
             inner join stage.user_bankrupt_relieve_stg t2
                on (t1.fbpid = t2.fbpid and t1.fgamefsk = 4132314431)
             where t2.dt = '%(statdate)s'
             distribute by t2.fbpid
              sort by fuid asc nulls last, t2.flts_at asc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

        return res


# 实例化执行
a = UserBankruptRelieveStg(sys.argv[1:])
a()