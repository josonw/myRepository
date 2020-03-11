#!/usr/local/python272/bin/python
# -*- coding: UTF-8 -*-

import os
import sys
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const


class JoinGamepartyStg(BaseStatModel):

    def create_tab(self):
        hql = """
            CREATE TABLE IF NOT EXISTS stage_dfqp.join_gameparty_stg(
             fbpid varchar(50) COMMENT 'BPID',
             fuid bigint COMMENT '用户ID',
             flts_at string COMMENT '报名时间',
             fname varchar(100) COMMENT '二级赛名称',
             fsubname varchar(50) COMMENT '三级赛名称',
             fentry_fee bigint COMMENT '报名费',
             fmatch_id varchar(100) COMMENT '赛事ID',
             fgame_id int COMMENT '子游戏ID',
             fchannel_code varchar(100) COMMENT '渠道邀请ID',
             fpname varchar(100) COMMENT '比赛场名称',
             fmode varchar(100) COMMENT '报名模式',
             ffirst tinyint COMMENT '首次报名某一级比赛场',
             ffirst_sub tinyint COMMENT '首次报名某二级比赛场',
             ffirst_match tinyint COMMENT '首次报名任意比赛场',
             ffirst_gsub int COMMENT '首次报名某三级赛事',
             fparty_type varchar(100) COMMENT '牌局类型',
             fitem_id varchar(100) COMMENT '物品ID',
             faward_type string COMMENT '奖励类型',
             fmatch_rule_type string COMMENT '赛制类型',
             fmatch_rule_id string COMMENT '赛制类型ID',
             fmatch_cfg_id int COMMENT '比赛配置ID',
             fmatch_log_id int COMMENT '比赛日志ID',
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
            COMMENT '地方棋牌比赛报名记录'
            PARTITIONED BY (dt string COMMENT '日期')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res

    def stat(self):
        hql = """
            insert overwrite table stage_dfqp.join_gameparty_stg partition (dt='%(statdate)s')
            select distinct t2.fbpid,
                   case
                     when t1.fplatformfsk = 77000221 and t2.fuid < 1000000000 then
                      t2.fuid + 1000000000
                     else
                      t2.fuid
                   end as fuid,
                   t2.flts_at,
                   t2.fname,
                   t2.fsubname,
                   t2.fentry_fee,
                   t2.fmatch_id,
                   t2.fgame_id,
                   t2.fchannel_code,
                   t2.fpname,
                   t2.fmode,
                   t2.ffirst,
                   t2.ffirst_sub,
                   t2.ffirst_match,
                   t2.ffirst_gsub,
                   t2.fparty_type,
                   t2.fitem_id,
                   t2.faward_type,
                   t2.fmatch_rule_type,
                   t2.fmatch_rule_id,
                   t2.fmatch_cfg_id,
                   t2.fmatch_log_id,
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
             inner join stage.join_gameparty_stg t2
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
a = JoinGamepartyStg(sys.argv[1:])
a()