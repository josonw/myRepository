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


class agg_match_event(BaseStatModel):
    def create_tab(self):
        hql = """
        create table if not exists bud_dm.match_event (
               fdate             date,
               fbpid             varchar(50),
               flts_at           varchar(50),
               fpname            varchar(100),
               fsubname          varchar(100),
               fgsubname         varchar(100),
               fmatch_id         varchar(100),
               fgame_id          int,
               fevent_id         varchar(100)
               )comment '赛事流程信息'
               partitioned by(dt date)
        location '/dw/bud_dm/match_event';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

    def stat(self):
        """ 重要部分，统计内容. """
        # 调试的时候把调试开关打开，正式执行的时候设置为0
        # self.debug = 1
        # 取基础数据
        hql = """insert overwrite table bud_dm.match_event
            partition(dt='%(statdate)s')
              select '%(statdate)s' fdate
                    ,fbpid
                    ,flts_at
                    ,fpname
                    ,fsubname
                    ,fgsubname
                    ,fmatch_id
                    ,fgame_id
                    ,fevent_id
                from stage.match_event_stg t
               where t.dt = '%(statdate)s';
        """
        res = self.sql_exe(hql)
        if res != 0:
            return res
        return res

# 生成统计实例
a = agg_match_event(sys.argv[1:])
a()
