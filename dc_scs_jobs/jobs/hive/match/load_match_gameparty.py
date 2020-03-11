# -*- coding: UTF-8 -*-
import os
import sys
import time
import datetime
from sys import path
path.append('%s/' % os.getenv('SCS_PATH'))
from PublicFunc import PublicFunc
from BaseStatModel import BaseStatModel
import service.sql_const as sql_const

class load_match_gameparty(BaseStatModel):
    def create_tab(self):
        """ 比赛参赛用户中间表
            2017-09-04 修改将 match_cfg_id与match_log_id组合成match_id"""
        hql = """create table if not exists dim.match_gameparty
            (fbpid              varchar(50)     comment 'BPID',
             fuid               bigint          comment '用户UID',
             fgame_id           int             comment '子游戏ID',
             fmatch_id          varchar(100)    comment '赛事id',
             ftbl_id            varchar(64)     comment '桌子编号',
             finning_id         varchar(64)     comment '牌局编号',
             fs_timer           varchar(100)    comment '开始时间戳',
             fe_timer           varchar(100)    comment '结束时间戳',
             fcharge            bigint          comment '台费',
             ffirst_play        int             comment '是否首次在该一级场次玩牌',
             ffirst_play_sub    int             comment '是否首次在该二级场次玩牌',
             ffirst_match       int             comment '首次参加比赛场',
             ffirst_play_gsub   int             comment '是否首次在该三级场次玩牌',
             fround_num         int             comment '轮数',
             fgame_num          int             comment '局数',
             fmatch_cfg_id      varchar(100)    comment '比赛配置id',
             fpname             varchar(100)    comment '牌局一级分类',
             fsubname           varchar(100)    comment '牌局二级分类',
             fgsubname          varchar(100)    comment '牌局三级分类',
             fparty_type        varchar(100)    comment '牌局类型',
             faward_type        varchar(100)    comment '奖励类型',
             fmatch_rule_type   varchar(100)    comment '赛制类型',
             fmatch_rule_id     varchar(100)    comment '赛制类型id',
             fis_fail           int             comment '是否开赛失败',
             fmode              int             comment '是否有偿报名参赛',
             fgamecoins         bigint          comment '输赢游戏币数量'
            )
            partitioned by (dt date)
        stored as orc
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分,统计内容 """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1

        res = self.sql_exe("""set hive.new.job.grouping.set.cardinality=100;SET hive.vectorized.execution.enabled=false;set hive.auto.convert.join=false;""")
        if res != 0:
            return res

        hql = """
        insert overwrite table dim.match_gameparty partition (dt = "%(statdate)s" )
                select  t1.fbpid
                       ,t1.fuid
                       ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                       ,t.fmatch_id fmatch_id
                       ,t1.ftbl_id
                       ,t1.finning_id
                       ,t1.fs_timer
                       ,t1.fe_timer
                       ,t1.fcharge
                       ,t1.ffirst_play
                       ,t1.ffirst_play_sub
                       ,t1.ffirst_match
                       ,t1.ffirst_play_gsub
                       ,t1.fround_num
                       ,t1.fgame_num
                       ,t.fmatch_cfg_id
                       ,t.fpname
                       ,t.fsubname
                       ,t.fgsubname
                       ,t.fparty_type
                       ,t.faward_type
                       ,t.fmatch_rule_type
                       ,t.fmatch_rule_id
                       ,t.fis_fail
                       ,case when t2.fentry_fee > 0 then 1 else 0 end fmode
                       ,t1.fgamecoins
                  from dim.match_config t
                  join (select * from stage.user_gameparty_stg t1
                         where t1.dt >= '%(statdate)s'
                           and t1.dt <= '%(ld_end)s' --有些比赛的牌局会跨天
                           and (coalesce(t1.fmatch_cfg_id,0)<>0 or coalesce(t1.fmatch_log_id,0)<>0)
                       ) t1
                    on t.fmatch_id = concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string))
                  left join (select concat_ws('_', cast (coalesce(t.fmatch_cfg_id,0) as string), cast (coalesce(t.fmatch_log_id,0) as string)) fmatch_id
                                    ,fuid
                                    ,coalesce(fentry_fee,0) fentry_fee
                                    ,row_number() over(partition by concat_ws('_', cast (t.fmatch_cfg_id as string), cast (t.fmatch_log_id as string)), fuid order by flts_at desc) row_num
                               from stage.join_gameparty_stg t
                              where dt = '%(statdate)s'
                                and (coalesce(t.fmatch_cfg_id,0)<>0 or coalesce(t.fmatch_log_id,0)<>0)
                       ) t2
                    on t2.fmatch_id = concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string))
                   and t1.fuid = t2.fuid
                   and t2.row_num = 1
                 where t.dt = '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_match_gameparty(sys.argv[1:])
a()
