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

class load_join_gameparty(BaseStatModel):
    def create_tab(self):
        """ 比赛报名用户中间表
            2017-09-04 修改将 match_cfg_id与match_log_id组合成match_id"""
        hql = """create table if not exists dim.join_gameparty
            (fbpid              varchar(50)       comment  'BPID',
             fuid               bigint            comment  '用户UID',
             fitem_id           varchar(100)      comment  '报名物品id',
             fentry_fee         bigint            comment  '报名费',
             fmatch_id          varchar(100)      comment  '比赛id',
             fgame_id           int               comment  '子游戏ID',
             ffirst             int               comment  '首次报名某一级比赛场',
             ffirst_sub         int               comment  '首次报名某二级比赛场',
             ffirst_match       int               comment  '首次报名任意比赛场',
             ffirst_gsub        int               comment  '首次报名某三级赛事',
             fmatch_cfg_id      varchar(100)      comment  '比赛配置id',
             fpname             varchar(100)      comment  '牌局一级分类',
             fsubname           varchar(100)      comment  '牌局二级分类',
             fgsubname          varchar(100)      comment  '牌局三级分类',
             fparty_type        varchar(100)      comment  '牌局类型',
             faward_type        varchar(100)      comment  '奖励类型',
             fmatch_rule_type   varchar(100)      comment  '赛制类型',
             fmatch_rule_id     varchar(100)      comment  '赛制类型id',
             fis_fail           int               comment  '是否开赛失败'
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
        insert overwrite table dim.join_gameparty partition (dt = "%(statdate)s" )
                select  t1.fbpid
                       ,t1.fuid
                       ,t1.fitem_id
                       ,t1.fentry_fee
                       ,concat_ws('_', cast (t1.fmatch_cfg_id as string), cast (t1.fmatch_log_id as string)) fmatch_id
                       ,coalesce(t1.fgame_id,cast (0 as bigint)) fgame_id
                       ,t1.ffirst
                       ,t1.ffirst_sub
                       ,t1.ffirst_match
                       ,t1.ffirst_gsub
                       ,coalesce(t.fmatch_cfg_id, cast (t1.fmatch_cfg_id as string),'%(null_str_report)s') fmatch_cfg_id
                       ,coalesce(t.fpname, t1.fpname,'%(null_str_report)s') fpname
                       ,coalesce(t.fsubname, t1.fname,'%(null_str_report)s') fsubname
                       ,coalesce(t.fgsubname, t1.fsubname,'%(null_str_report)s') fgsubname
                       ,coalesce(t.fparty_type, t1.fparty_type,'%(null_str_report)s') fparty_type
                       ,coalesce(t.faward_type, t1.faward_type,'%(null_str_report)s') faward_type
                       ,coalesce(t.fmatch_rule_type, t1.fmatch_rule_type,'%(null_str_report)s') fmatch_rule_type
                       ,coalesce(t.fmatch_rule_id, t1.fmatch_rule_id,'%(null_str_report)s') fmatch_rule_id
                       ,t.fis_fail
                  from stage.join_gameparty_stg t1
                  left join dim.match_config t
                    on t.fmatch_id = concat_ws('_', cast (coalesce(t1.fmatch_cfg_id,0) as string), cast (coalesce(t1.fmatch_log_id,0) as string))
                   and t.dt = '%(statdate)s'
                 where t1.dt = '%(statdate)s'
                   and (coalesce(t1.fmatch_cfg_id,0)<>0 or coalesce(t1.fmatch_log_id,0)<>0)

        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_join_gameparty(sys.argv[1:])
a()
