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


class load_dfqp_quit_gameparty_stg(BaseStatModel):

    # 将地方棋牌的退赛流水单独剥离出来
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.quit_gameparty_stg
        ( 	fbpid					varchar(50) 	comment 'BPID',
			fuid                    bigint          comment '用户UID',
			flts_at                 string          comment '发生时间',
			fpname                  varchar(100)    comment '比赛场名称',
			fsubname                varchar(100)    comment '二级赛场名称（比赛房间）',
			fgsubname               varchar(100)    comment '三级赛场名称（具体赛事）',
			fmatch_id               varchar(100)    comment '比赛id',
			fcause                  int             comment '退赛原因',
			fgame_id                int             comment '子游戏ID',
			fchannel_code           varchar(100)    comment '渠道邀请ID',
			fround_num              int             comment '轮数',
			fgame_num               int             comment '局数',
			fpart_rank              int             comment '本局名次',
			fintegral_balance       int             comment '退赛时的积分结余',
			fintegral_balance_ext   string          comment '退赛时其他玩家的积分结余，格式：玩家1的mid，积分结余；玩家2的mid，积分结余',
			fmatch_cfg_id           int             comment '比赛配置id',
			fmatch_log_id           int             comment '比赛日志id',
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
        ) comment '地方棋牌退出比赛流水'
        partitioned by(dt string comment '日期')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.quit_gameparty_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid,
				case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid,
				flts_at,
				fpname,
				fsubname,
				fgsubname,
				fmatch_id,
				fcause,
				fgame_id,
				fchannel_code,
				fround_num,
				fgame_num,
				fpart_rank,
				fintegral_balance,
				fintegral_balance_ext,
				fmatch_cfg_id,
				fmatch_log_id,
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
        from stage.quit_gameparty_stg t1
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
a = load_dfqp_quit_gameparty_stg(sys.argv[1:])
a()
