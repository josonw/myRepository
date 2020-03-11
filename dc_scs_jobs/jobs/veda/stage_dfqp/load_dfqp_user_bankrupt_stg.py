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


class load_dfqp_user_bankrupt_stg(BaseStatModel):

    # 将地方棋牌的用户破产流水单独剥离出来
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.user_bankrupt_stg
        ( 	fbpid					varchar(50) 	comment 'BPID',
			fuid                    bigint          comment '用户UID',
			frupt_at                string          comment '破产时间',
			fhas_relieve            bigint          comment '是否接受救济',
			fuser_grade             bigint          comment '用户等级',
			frelieve_game_coin      bigint          comment '救济游戏币额度',
			frelieve_cnt            int             comment '第几次数救济',
			fuphill_pouring         bigint          comment '底注/小盲',
			fplayground_title       varchar(100)    comment '牌局二级场次',
			fversion_info           varchar(50)     comment '版本号',
			fchannel_code           varchar(100)    comment '渠道邀请ID',
			fvip_type               varchar(100)    comment '',
			fvip_level              int             comment '',
			flevel                  int             comment '',
			fpname                  varchar(100)    comment '牌局一级场次',
			fscene                  varchar(100)    comment '破产场景',
			fgame_id                int             comment '子游戏ID',
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
        ) comment '地方棋牌用户破产流水'
        partitioned by(dt string comment '日期')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.user_bankrupt_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid,
				case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid,
				frupt_at,
				fhas_relieve,
				fuser_grade,
				frelieve_game_coin,
				frelieve_cnt,
				fuphill_pouring,
				fplayground_title,
				fversion_info,
				fchannel_code,
				fvip_type,
				fvip_level,
				flevel,
				fpname,
				fscene,
				fgame_id,
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
        from stage.user_bankrupt_stg t1
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
a = load_dfqp_user_bankrupt_stg(sys.argv[1:])
a()
