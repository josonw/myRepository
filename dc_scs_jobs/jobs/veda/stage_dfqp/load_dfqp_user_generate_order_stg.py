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


class load_dfqp_user_generate_order_stg(BaseStatModel):

    # 将地方棋牌的用户下单流水单独剥离出来
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.user_generate_order_stg
        ( 	fbpid					varchar(50)  	comment 'BPID',
			fuid                    bigint       	comment '用户UID',
			fplatform_uid           varchar(50)  	comment '平台用户id',
			fentrance_id            varchar(20)  	comment '登入入口',
			forder_at               string       	comment '订单时间',
			forder_id               varchar(100) 	comment '支付订单号',
			fcurrency_type          smallint     	comment '币种类型',
			fcurrency_num           smallint     	comment '币种数量',
			fitem_category          smallint     	comment '购买商品类别',
			fitem_id                bigint       	comment '购买物品id',
			fitem_num               bigint       	comment '购买物品数量',
			fbalance                bigint       	comment '下单时用户身上有多少游戏币',
			fgrade                  int          	comment '下单时用户当前等级',
			fgameparty_pname        varchar(50)  	comment '下单时牌局一级场次',
			fgameparty_subname      varchar(50)  	comment '下单时牌局二级场次',
			fgameparty_anto         bigint       	comment '下单时底注场',
			fbankrupt               tinyint      	comment '下单时用户是否处于破产状态',
			fpay_scene              varchar(100) 	comment '付费场景',
			fip                     varchar(100) 	comment '用户下单ip地址',
			fplatform_order_id      varchar(100) 	comment '平台订单流水号',
			fb_order_id             bigint       	comment '业务内部订单号',
			ffee                    decimal(20,2)	comment '手续费用',
			fpm_name                varchar(100) 	comment '下单时所用的支付方式名称',
			fversion_info           varchar(50)  	comment '下单的游戏版本号',
			fchannel_code           varchar(100) 	comment '渠道邀请ID',
			fgame_id                int          	comment '子游戏ID',
			fpartner_info           varchar(32)  	comment '合作属性',
			fgameparty_gsubname     string       	comment '',
			fpay_scene_type         string       	comment '付费场景类型',
			faward_type             string       	comment '奖励类型',
			fparty_type             string       	comment '下单时赛事类型',
			fpay_scene_extra        string       	comment '付费场景补充字段',
			fmatch_rule_type        string          comment '赛制类型',
			fmatch_rule_id          string          comment '赛制类型id',
			fpay_scene_text         string          comment '新增字段场景类型说明',
			fpromoter_id            string          comment '推广员id',
			fgamefsk				bigint			comment '游戏ID',
			fgamename				string			comment '游戏名称',
			fplatformfsk			bigint			comment '平台ID',
			fplatformname			string			comment '平台名称',
			fhallfsk				bigint			comment '大厅ID',
			fhallname				string			comment '大厅名称',
			fterminaltypefsk		bigint			comment '终端ID',
			fterminaltypename		string			comment '终端名称',
			fversionfsk				bigint			comment '版本ID',
			fversionname			string			comment '版本名称'
        ) comment '地方棋牌用户下单流水'
        partitioned by(dt string comment '日期')
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.user_generate_order_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid,
				case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid,
				fplatform_uid,
				fentrance_id,
				forder_at,
				forder_id,
				fcurrency_type,
				fcurrency_num,
				fitem_category,
				fitem_id,
				fitem_num,
				fbalance,
				fgrade,
				fgameparty_pname,
				fgameparty_subname,
				fgameparty_anto,
				fbankrupt,
				fpay_scene,
				fip,
				fplatform_order_id,
				fb_order_id,
				ffee,
				fpm_name,
				fversion_info,
				fchannel_code,
				fgame_id,
				fpartner_info,
				fgameparty_gsubname,
				fpay_scene_type,
				faward_type,
				fparty_type,
				fpay_scene_extra,
				fmatch_rule_type,
				fmatch_rule_id,
				fpay_scene_text,
				fpromoter_id,
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
        from stage.user_generate_order_stg t1
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
a = load_dfqp_user_generate_order_stg(sys.argv[1:])
a()
