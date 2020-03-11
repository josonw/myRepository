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


class load_dfqp_user_gameparty_stg(BaseStatModel):

    # 将地方棋牌的牌局流水单独剥离出来
    def create_tab(self):
        hql = """
        create table if not exists stage_dfqp.user_gameparty_stg
        ( fbpid              varchar(50),
          flts_at            string          comment '牌局上报时间戳',
          fuid               bigint          comment '用户UID',
          ftbl_id            varchar(64)     comment '桌子编号',
          finning_id         varchar(64)     comment '牌局编号',
          fpalyer_cnt        smallint        comment '参与牌局人数',
          fs_timer           string          comment '开始时间戳',
          fe_timer           string          comment '结束时间戳',
          fgamecoins         bigint          comment '输赢游戏币数量',
          fuser_gcoins       bigint          comment '结余游戏币数量',
          fcharge            decimal(20,2)   comment '台费',
          fsubname           varchar(100)    comment '牌局二级场次',
          fpname             varchar(100)    comment '牌局一级场次',
          fblind_1           bigint          comment '底注/小盲注（德州）',
          fblind_2           bigint          comment '大盲注（德州）',
          flimit_gcoins      bigint          comment '进场最小限额',
          fhas_fold          tinyint         comment '是否弃牌',
          fhas_open          tinyint         comment '是否亮牌',
          frobots_num        tinyint         comment '机器人数量',
          ftrustee_num       smallint        comment '使用托管次数',
          fis_king           tinyint         comment '是否为庄家',
          fserv_charge       bigint          comment '牌局服务费',
          fcard_type         varchar(50)     comment '牌型',
          fversion_info      varchar(50)     comment '版本号',
          fchannel_code      varchar(100)    comment '渠道邀请ID',
          fintegral_val      bigint          comment '积分变化量',
          fintegral_balance  bigint          comment '积分结余',
          fis_weedout        tinyint         comment '是否淘汰',
          fis_bankrupt       tinyint         comment '是否破产',
          fis_end            tinyint         comment '结束标识',
          fmatch_id          varchar(100)    comment '赛事id',
          fms_time           string          comment '比赛开始时间',
          fme_time           string          comment '比赛结束时间',
          fvip_type          varchar(100)    comment 'VIP类型',
          fvip_level         bigint          comment 'VIP等级',
          flevel             bigint          comment '等级',
          fgsubname          varchar(128)    comment '牌局三级场次',
          ftbuymin           int             comment '最小携带',
          ftbuymax           int             comment '最大买入',
          fprechip           int             comment '必下注数额（德州）',
          ffirst_play        tinyint         comment '是否首次在该一级场次玩牌',
          fgame_id           int             comment '子游戏ID',
          ffirst_play_sub    tinyint         comment '是否首次在该二级场次玩牌',
          ffirst_match       tinyint         comment '首次参加比赛场',
          fcoins_type        int             comment '货币类型',
          fparty_type        varchar(100)    comment '牌局类型',
          ffirst_play_gsub   int             comment '是否首次在该三级场次玩牌',
          fshift_party       int             comment '牌局自动换场',
          fpartner_info      varchar(32),
          fbank_gamecoins    bigint          comment '保险箱游戏币额度',
          fbank_boyaacoins   bigint          comment '保险箱博雅币额度',
          fbank_currency1    bigint,
          fbank_currency2    bigint,
          fbank_currency3    bigint,
          faward_type        string          comment '奖励类型',
          fround_num         int             comment '轮数',
          fgame_num          int             comment '局数',
          fmatch_cfg_id      int             comment '比赛配置id',
          fmatch_log_id      int             comment '比赛日志id',
          froom_id           string          comment '房间id',
          fmultiple          int             comment '倍数（正数表示赢的，负数表示输的）',
          fgamefsk           bigint          comment '游戏id',
          fgamename          string          comment '游戏名称',
          fplatformfsk       bigint          comment '平台id',
          fplatformname      string          comment '平台名称',
          fhallfsk           bigint          comment '大厅id',
          fhallname          string          comment '大厅名称',
          fterminaltypefsk   bigint          comment '终端类型id',
          fterminaltypename  string          comment '终端类型名称',
          fversionfsk        bigint          comment '版本id',
          fversionname       string          comment '版本名称'
        )comment '地方棋牌牌局流水'
        partitioned by(dt string)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):

        hql = """
        insert overwrite table stage_dfqp.user_gameparty_stg
        partition(dt='%(statdate)s')
        select distinct t1.fbpid,
               flts_at,
               case when tt.fplatformfsk in (77000221) and fuid < 1000000000 then fuid + 1000000000 else fuid end fuid,
               ftbl_id,
               finning_id,
               fpalyer_cnt,
               fs_timer,
               fe_timer,
               fgamecoins,
               fuser_gcoins,
               fcharge,
               fsubname,
               fpname,
               fblind_1,
               fblind_2,
               flimit_gcoins,
               fhas_fold,
               fhas_open,
               frobots_num,
               ftrustee_num,
               fis_king,
               fserv_charge,
               fcard_type,
               fversion_info,
               fchannel_code,
               fintegral_val,
               fintegral_balance,
               fis_weedout,
               fis_bankrupt,
               fis_end,
               fmatch_id,
               fms_time,
               fme_time,
               fvip_type,
               fvip_level,
               flevel,
               fgsubname,
               ftbuymin,
               ftbuymax,
               fprechip,
               ffirst_play,
               fgame_id,
               ffirst_play_sub,
               ffirst_match,
               fcoins_type,
               fparty_type,
               ffirst_play_gsub,
               fshift_party,
               fpartner_info,
               fbank_gamecoins,
               fbank_boyaacoins,
               fbank_currency1,
               fbank_currency2,
               fbank_currency3,
               faward_type,
               fround_num,
               fgame_num,
               fmatch_cfg_id,
               fmatch_log_id,
               froom_id,
               fmultiple,
               tt.fgamefsk,
               tt.fgamename,
               tt.fplatformfsk,
               tt.fplatformname,
               tt.fhallfsk,
               tt.fhallname,
               tt.fterminaltypefsk,
               tt.fterminaltypename,
               tt.fversionfsk,
               tt.fversionname
        from stage.user_gameparty_stg t1
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
a = load_dfqp_user_gameparty_stg(sys.argv[1:])
a()
