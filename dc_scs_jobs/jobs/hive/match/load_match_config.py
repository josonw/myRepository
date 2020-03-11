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

class load_match_config(BaseStatModel):
    def create_tab(self):
        """ 比赛配置中间表
            2017-09-04 修改将 match_cfg_id与match_log_id组合成match_id"""
        hql = """create table if not exists dim.match_config
            (
             fmatch_id          varchar(100)    comment '赛事id',
             fmatch_cfg_id      varchar(100)    comment '比赛配置id',
             fpname             varchar(100)    comment '牌局一级分类',
             fsubname           varchar(100)    comment '牌局二级分类',
             fgsubname          varchar(100)    comment '牌局三级分类',
             fparty_type        varchar(100)    comment '牌局类型',
             fmode              varchar(100)    comment '报名模式',
             faward_type        varchar(100)    comment '奖励类型',
             fmatch_rule_type   varchar(100)    comment '赛制类型',
             fmatch_rule_id     varchar(100)    comment '赛制类型id',
             fis_fail           int             comment '是否开赛失败',
             fbigin_at          varchar(100)    comment '比赛开始时间',
             fend_at            varchar(100)    comment '比赛结束时间',
             fwin_num           int             comment '奖励圈大小',
             fvictory_num       int             comment '决胜圈大小',
             fjoin_unum         int             comment '报名人数',
             fparty_unum        int             comment '比赛人数'
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
        insert overwrite table dim.match_config partition (dt = "%(statdate)s" )
 select fmatch_id
        ,fmatch_cfg_id
        ,fpname
        ,fsubname
        ,fgsubname
        ,fparty_type
        ,fmode
        ,faward_type
        ,fmatch_rule_type
        ,fmatch_rule_id
        ,fis_fail
        ,fbigin_at
        ,fend_at
        ,fwin_num
        ,fvictory_num
        ,fjoin_unum
        ,fparty_unum
   from (
         select t.fmatch_id
                ,max(fmatch_cfg_id) fmatch_cfg_id
                ,max(fpname) fpname
                ,max(fsubname) fsubname
                ,max(fgsubname) fgsubname
                ,max(fparty_type) fparty_type
                ,max(fmode) fmode
                ,max(faward_type) faward_type
                ,max(fmatch_rule_type) fmatch_rule_type
                ,max(fmatch_rule_id) fmatch_rule_id
                ,max(fis_fail) fis_fail
                ,min(fbigin_at) fbigin_at
                ,max(fend_at) fend_at
                ,max(fwin_num) fwin_num
                ,max(fvictory_num) fvictory_num
                ,max(fjoin_unum) fjoin_unum
                ,max(fparty_unum) fparty_unum
           from (
                select concat_ws('_', cast (coalesce(t.fmatch_cfg_id,0) as string), cast (coalesce(t.fmatch_log_id,0) as string)) fmatch_id
                       ,max(fmatch_cfg_id) fmatch_cfg_id
                       ,max(fpname) fpname
                       ,max(fsubname) fsubname
                       ,max(fgsubname) fgsubname
                       ,max(fparty_type) fparty_type
                       ,max(fmode) fmode
                       ,max(faward_type) faward_type
                       ,max(fmatch_rule_type) fmatch_rule_type
                       ,max(fmatch_rule_id) fmatch_rule_id
                       ,max(case when fparty_type = '快速赛' then 0 else fis_fail end) fis_fail
                       ,min(fbigin_at) fbigin_at
                       ,max(fend_at) fend_at
                       ,max(fwin_num) fwin_num
                       ,max(fvictory_num) fvictory_num
                       ,0 fjoin_unum
                       ,0 fparty_unum
                  from stage.match_config_stg t
                 where t.dt = '%(statdate)s'
                   and (coalesce(t.fmatch_cfg_id,0)<>0 or coalesce(t.fmatch_log_id,0)<>0)
                 group by concat_ws('_', cast (coalesce(t.fmatch_cfg_id,0) as string), cast (coalesce(t.fmatch_log_id,0) as string))
                 union all
                select concat_ws('_', cast (coalesce(t.fmatch_cfg_id,0) as string), cast (coalesce(t.fmatch_log_id,0) as string)) fmatch_id
                       ,'%(null_str_report)s' fmatch_cfg_id
                       ,max(fpname) fpname
                       ,max(fname) fsubname
                       ,max(fsubname) fgsubname
                       ,max(fparty_type) fparty_type
                       ,'%(null_str_report)s' fmode
                       ,max(faward_type) faward_type
                       ,max(fmatch_rule_type) fmatch_rule_type
                       ,max(fmatch_rule_id) fmatch_rule_id
                       ,0 fis_fail
                       ,null fbigin_at
                       ,null fend_at
                       ,0 fwin_num
                       ,0 fvictory_num
                       ,count(distinct fuid) fjoin_unum
                       ,0 fparty_unum
                  from stage.join_gameparty_stg t
                 where t.dt = '%(statdate)s'
                   and (coalesce(t.fmatch_cfg_id,0)<>0 or coalesce(t.fmatch_log_id,0)<>0)
                 group by concat_ws('_', cast (coalesce(t.fmatch_cfg_id,0) as string), cast (coalesce(t.fmatch_log_id,0) as string))
                 union all
                select concat_ws( '_', cast (coalesce(t.fmatch_cfg_id,0) as string),cast (coalesce(t.fmatch_log_id,0) as string)) fmatch_id
                       ,'%(null_str_report)s' fmatch_cfg_id
                       ,max(fpname) fpname
                       ,max(fsubname) fsubname
                       ,max(fgsubname) fgsubname
                       ,max(fparty_type) fparty_type
                       ,'%(null_str_report)s' fmode
                       ,'%(null_str_report)s' faward_type
                       ,'%(null_str_report)s' fmatch_rule_type
                       ,'%(null_str_report)s' fmatch_rule_id
                       ,0 fis_fail
                       ,null fbigin_at
                       ,null fend_at
                       ,0 fwin_num
                       ,0 fvictory_num
                       ,0 fjoin_unum
                       ,count(distinct fuid) fparty_unum
                  from stage.user_gameparty_stg t
                 where t.dt = '%(statdate)s'
                   and (coalesce(t.fmatch_cfg_id,0)<>0 or coalesce(t.fmatch_log_id,0)<>0)
                 group by concat_ws('_', cast (coalesce(t.fmatch_cfg_id,0) as string), cast (coalesce(t.fmatch_log_id,0) as string))
                 ) t
         group by t.fmatch_id
        ) t where fbigin_at is not null and fbigin_at >= '%(statdate)s'
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = load_match_config(sys.argv[1:])
a()
