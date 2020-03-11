#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class MatchEconomyStg(BaseStatModel):

    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS stage_dfqp.match_economy_stg(
             fbpid varchar(50) COMMENT 'BPID',
             fuid bigint COMMENT '用户ID',
             flts_at string COMMENT '发生时间',
             fpname varchar(100) COMMENT '比赛场名称',
             fsubname varchar(100) COMMENT '二级赛名称',
             fgsubname varchar(100) COMMENT '三级赛名称',
             fmatch_id varchar(100) COMMENT '赛事ID',
             fio_type int COMMENT '操作类型：1-发放，2-消耗',
             fitem_type int COMMENT '物品类型',
             fitem_id varchar(100) COMMENT '物品ID',
             fitem_num bigint COMMENT '物品数量',
             fgame_id int COMMENT '子游戏ID',
             fchannel_code varchar(100) COMMENT '渠道邀请码',
             fcost decimal(20,2) COMMENT '配置成本(现金价值)',
             fact_id int COMMENT '途径ID：1报名，2奖励，3轮间/胜局奖励，4取消报名，5复活，6投注赛抽水(废弃)，7除报名费外的奖池',
             frank int COMMENT '获奖名次',
             fact_desc string COMMENT '详细变化原因',
             fround_num int COMMENT '轮数',
             fgame_num int COMMENT '局数',
             fmatch_cfg_id int COMMENT '比赛配置ID',
             fmatch_log_id int COMMENT '比赛日志ID',
             fpurchase_cost decimal(20,2) COMMENT '采购成本',
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
            COMMENT '地方棋牌比赛场发放消耗'
            PARTITIONED BY (dt string COMMENT '日期')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            insert overwrite table stage_dfqp.match_economy_stg partition (dt='%(statdate)s')
            select distinct t2.fbpid,
                   case
                     when t1.fplatformfsk = 77000221 and t2.fuid < 1000000000 then
                      t2.fuid + 1000000000
                     else
                      t2.fuid
                   end as fuid,
                   t2.flts_at,
                   t2.fpname,
                   t2.fsubname,
                   t2.fgsubname,
                   t2.fmatch_id,
                   t2.fio_type,
                   t2.fitem_type,
                   t2.fitem_id,
                   t2.fitem_num,
                   t2.fgame_id,
                   t2.fchannel_code,
                   t2.fcost,
                   t2.fact_id,
                   t2.frank,
                   t2.fact_desc,
                   t2.fround_num,
                   t2.fgame_num,
                   t2.fmatch_cfg_id,
                   t2.fmatch_log_id,
                   t2.fpurchase_cost,
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
             inner join stage.match_economy_stg t2
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
a = MatchEconomyStg(sys.argv[1:])
a()